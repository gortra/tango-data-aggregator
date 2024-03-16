import asyncio
import os
import time

import h5py
import numpy as np
import tango
import tango.server as ts


# ----------------------------------------------------------------------
# helper methods and utility classes
# ----------------------------------------------------------------------
def is_start_allowed(device):
    if device.get_state() == tango.DevState.ON:
        return True
    else:
        return False


def is_stop_allowed(device):
    if device.get_state() == tango.DevState.ON:
        return True
    else:
        return False


class DAData:
    def __init__(self, attr_id, attr_key,
                 attr_type="run_attribute",
                 size_buffer=None
                 ):
        self.attr_id = attr_id
        self.attr_key = attr_key
        self.attr_type = attr_type
        self.attr_proxy = tango.AttributeProxy(attr_id)

        self._event_id = None
        if attr_type == "push_attribute":
            self._event_id = self.attr_proxy.subscribe_event(
                tango.EventType.CHANGE_EVENT,
                size_buffer)

        self._d_attr_obj = self.attr_proxy.read()

        self.last_error = None

    @property
    def dim_x(self):
        return self._d_attr_obj.dim_x

    @property
    def dim_y(self):
        return self._d_attr_obj.dim_y

    @property
    def data_format(self):
        return self._d_attr_obj.data_format.name

    @property
    def data_type(self):
        if self._d_attr_obj.data_format.name == "SCALAR":
            return type(self._d_attr_obj.value)
        else:
            return self._d_attr_obj.value.dtype

    @property
    def data_shape(self):
        """

        Returns
        -------
        tuple
        """
        d_shape = (1, self._d_attr_obj.dim_x)
        if self._d_attr_obj.dim_y > 0:
            d_shape = d_shape + (self._d_attr_obj.dim_y,)
        return d_shape

    @property
    def data_shape_max(self):
        """

        Returns
        -------
        tuple
        """
        d_shape = self.data_shape
        d_shape_max = (None,) + d_shape[1::]
        return d_shape_max

    def get_data(self):
        """Poll newest data

        Returns
        -------
        data_new : list of tuple
            [(val_1, timestamp_1), .... , (val_n, timestamp_n)]
        """
        data_new = []
        if self.attr_type == "push_attribute":
            events = self.attr_proxy.get_events(self._event_id)
            for e in events:
                if not e.err:
                    attr_obj = e.attr_value
                    attr_obj.timestamp = attr_obj.time.totime()
                    data_new.append((attr_obj.value, attr_obj.timestamp))
        else:
            try:
                d_attr_obj = self.attr_proxy.read()
                timestamp = d_attr_obj.time.totime()
                val = d_attr_obj.value
                data_new.append((val, timestamp))
                self.last_error = None
            except (tango.DevFailed, tango.ConnectionFailed) as err:
                # ToDo: More detailed error handling
                self.last_error = err
            except tango.DevError:
                # ToDo: More detailed error handling
                pass

        return data_new

    def push_event(self):
        # used in subscribe_event() callback mode
        pass


# ----------------------------------------------------------------------
# device class
# ----------------------------------------------------------------------
class DataAggregator(ts.Device):
    green_mode = tango.GreenMode.Asyncio

    # ------------------------------------------------------------------
    # internal variables
    # ------------------------------------------------------------------
    _v_init = ("Attributes", ({"init": ["", "", ""]}))
    _var_run = _v_init
    _var_poll = _v_init
    _var_push = _v_init
    _proxy_handles = []
    _run_task = None

    _name_da = ""
    _file_number = 1
    _file_path = ""
    _file_name = ""
    _file_size = 0.0

    _poll_period = 3.0
    _cycle_duration = 0.0
    _n_buffer_current = 0

    # reduce maximum buffer size in order to avoid memory problems
    _size_buffer_stream = 1000

    # ------------------------------------------------------------------
    # class and device properties
    # ------------------------------------------------------------------
    buffer_size = ts.device_property(dtype=int, default_value=1000)

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    async def init_device(self):
        await super().init_device()
        self.debug_stream(f'initialize device')
        self.set_state(tango.DevState.INIT)

        self._name_da = self.get_name().split("/")[-1]
        self._size_buffer_stream = self.buffer_size

        self.set_state(tango.DevState.ON)

    # ------------------------------------------------------------------
    # attributes
    # ------------------------------------------------------------------
    @ts.pipe(label="Run Attributes")
    async def attrs_run(self):
        return self._var_run

    @attrs_run.write
    async def attrs_run(self, attrs_run):
        self._var_run = attrs_run

    @ts.pipe(label="Polled Attributes")
    async def attrs_poll(self):
        return self._var_poll

    @attrs_poll.write
    async def attrs_poll(self, attrs_poll):
        self._var_poll = attrs_poll

    @ts.pipe(label="Pushed Attributes")
    async def attrs_push(self):
        return self._var_push

    @attrs_push.write
    async def attrs_push(self, attrs_push):
        self._var_push = attrs_push

    @ts.attribute(
        label="Polling period",
        memorized=True,
        dtype=float,
        unit='s',
    )
    async def polling_period(self):
        return self._poll_period

    @polling_period.write
    async def polling_period(self, polling_period):
        self._poll_period = polling_period

    @ts.attribute(
        label="File Path",
        memorized=True,
        dtype=str,
    )
    async def file_path(self):
        return self._file_path

    @file_path.write
    async def file_path(self, file_path):
        self._file_path = file_path
        self._file_number = 1

    @ts.attribute(
        label="File Name",
        dtype=str,
    )
    async def file_name(self):
        return self._file_name

    @ts.attribute(
        label="File Size",
        dtype=float,
        unit="MB",
        format="%3.3f",
    )
    async def file_size(self):
        return self._file_size

    @ts.attribute(
        label="Cycle processing time",
        unit="ms",
        dtype=float,
    )
    async def cycle_duration(self):
        return self._cycle_duration * 1000.0

    @ts.attribute(
        label="Maximum buffer load",
        dtype=float,
        unit="%",
        format="%3.2f",
    )
    async def buffer_load(self):
        return self._n_buffer_current / self._size_buffer_stream * 100.0

    # ------------------------------------------------------------------
    # commands
    # ------------------------------------------------------------------
    @ts.command(fisallowed=is_start_allowed)
    async def Start(self):
        self.debug_stream('start measurement, ...')
        self.set_status("starting .....")

        self._run_task = set()
        task = asyncio.create_task(self._da_thread())
        self._run_task.add(task)
        task.add_done_callback(self._run_task.discard)

        self.set_state(tango.DevState.RUNNING)

    @ts.command()
    async def Stop(self):
        self.debug_stream('stop measurement')
        self.set_status("stopping .....")

        try:
            task = self._run_task.pop()
            task.cancel()
            with h5py.File(self._file_name, 'a') as f:
                f.attrs["time_stop"] = time.time()
            del self._proxy_handles
            self._proxy_handles = []

        except asyncio.CancelledError as err:
            self.debug_stream(f"cancelling run-task: {err}")
            pass

        except (KeyError, AttributeError):
            self.warn_stream("tried to cancel a non-existent task ....")
            self._proxy_handles = []
            pass

        self.set_state(tango.DevState.ON)

    # ------------------------------------------------------------------
    # internal methods
    # ------------------------------------------------------------------
    async def _da_thread(self):
        """
        Main thread of data aggregator.

        Note
        ----
        wait some time before starting the actual thread. This is necessary
        in order to avoid timeouts on a run configurator, when using multiple
        instances running on the same device server.
        """
        await asyncio.sleep(3.0)

        # connect to attribute proxies
        attr_fail = []
        self._proxy_handles = []

        for attr in self._var_run[1]:
            tango_id = attr['value'][0] + "/" + attr['value'][1]
            try:
                pxy_handle = DAData(
                    tango_id,
                    attr['name'],
                    attr_type="run_attribute"
                )
                self._proxy_handles.append(pxy_handle)
            except tango.DevFailed:
                attr_fail.append(tango_id)
        self.debug_stream("created proxies for run attributes, ...")

        for attr in self._var_poll[1]:
            tango_id = attr['value'][0] + "/" + attr['value'][1]
            try:
                pxy_handle = DAData(
                    tango_id,
                    attr['name'],
                    attr_type="poll_attribute"
                )
                self._proxy_handles.append(pxy_handle)
            except tango.DevFailed:
                attr_fail.append(tango_id)
        self.debug_stream("created proxies for poll attributes, ...")

        for attr in self._var_push[1]:
            tango_id = attr['value'][0] + "/" + attr['value'][1]
            try:
                pxy_handle = DAData(
                    tango_id,
                    attr['name'],
                    attr_type="push_attribute",
                    size_buffer=self._size_buffer_stream
                )
                self._proxy_handles.append(pxy_handle)
            except tango.DevFailed:
                attr_fail.append(tango_id)

        self.debug_stream("created proxies for stream attributes, ...")
        self.set_status(f"Recording. Discard: {str(attr_fail)}")

        # initialize hdf5 file
        self._file_name = await self._init_file()

        # remove run attributes
        for p in list(self._proxy_handles):
            if p.attr_type == "run_attribute":
                self._proxy_handles.remove(p)

        # start recording
        while True:
            self._file_size = os.stat(self._file_name).st_size / (1024 ** 2)

            self.debug_stream("Wait...")
            await asyncio.sleep(self._poll_period)
            t_start = time.time()

            self.debug_stream("capturing polled data...")
            await self._store_data()
            t_cycle = time.time() - t_start
            self._cycle_duration = (self._cycle_duration + t_cycle) / 2.0
            self.debug_stream(f"processed chunk in {self._cycle_duration} s")

    async def _init_file(self):
        """
        Initialize hdf5 file and create first entries.

        Returns
        -------
        filename : str
        """
        if os.path.isfile(f"{self._file_path}/{self._name_da}_{self._file_number}.h5"):
            self._file_number += 1
        filename = f"{self._file_path}/{self._name_da}_{self._file_number}.h5"
        t_start = time.time()

        with h5py.File(filename, 'a') as f:
            self.info_stream(f"creating {filename}")

            f.create_group(f"data_run")
            f.create_group(f"data_recorded")

            for p_handle in self._proxy_handles:
                attr_type = {
                    "run_attribute": "data_run",
                    "poll_attribute": "data_recorded",
                    "push_attribute": "data_recorded",
                }[p_handle.attr_type]
                group_name = f"{attr_type}/{p_handle.attr_key}"
                f.create_group(group_name)
                await self._create_dset(f, p_handle, group_name)

            f.attrs['time_start'] = t_start
            f.attrs['name_experiment'] = self._file_path.split("/")[-2]
            f.attrs['poll_period'] = self._poll_period
            f.attrs['size_buffer_stream'] = self._size_buffer_stream

        return filename

    async def _create_dset(self, f, p_handle, group_name):
        if p_handle.attr_type == "run_attribute":
            self.debug_stream("initialize run attributes ...")
            val, tstamp = p_handle.get_data()[0]

            f[group_name].create_dataset(
                'timestamp',
                data=tstamp
            )

            f[group_name].create_dataset(
                'data',
                data=val
            )

        else:
            self.debug_stream("initialize poll and push attributes ...")
            val, tstamp = p_handle.get_data()[0]

            f[group_name].create_dataset(
                "timestamp",
                (1, 1),
                maxshape=(None, 1),
                dtype=float
            )
            f[f"{group_name}/timestamp"][0] = tstamp

            f[group_name].create_dataset(
                "data",
                p_handle.data_shape,
                maxshape=p_handle.data_shape_max,
                dtype=p_handle.data_type
            )
            f[f"{group_name}/data"][0] = val

        f[group_name].attrs['attribute_id'] = p_handle.attr_id
        f[group_name].attrs['attribute_type'] = p_handle.attr_type

    async def _store_data(self):
        with h5py.File(self._file_name, 'a') as f:
            self.debug_stream("dumping data ....")
            n_buffer_max = 0
            error_dev = []

            for p_handle in self._proxy_handles:
                data_new = p_handle.get_data()
                n_new = len(data_new)
                if p_handle.last_error is not None:
                    self.debug_stream(f"{p_handle.last_error}")
                if n_new == 0:
                    error_dev.append(p_handle.attr_key)
                    self.debug_stream("no data captured")
                    continue
                n_buffer_max = max(n_new, n_buffer_max)

                t_new = np.zeros((n_new, 1), dtype=float)
                d_new = np.zeros(
                    (n_new,) + p_handle.data_shape[1::],
                    dtype=p_handle.data_type
                )

                for i, d in zip(range(n_new), data_new):
                    t_new[i] = d[1]
                    d_new[i] = d[0]

                group_name = f"data_recorded/{p_handle.attr_key}"
                dset = f[f"{group_name}/timestamp"]
                dset.resize(dset.shape[0]+n_new, axis=0)
                dset[-n_new::] = t_new

                dset = f[f"{group_name}/data"]
                dset.resize(dset.shape[0]+n_new, axis=0)
                dset[-n_new::] = d_new

            self._n_buffer_current = n_buffer_max
            status_string = "Recording ....."
            if error_dev:
                status_string = f"Recording. Problem occurred: {str(error_dev)}"
            self.set_status(status_string)


if __name__ == "__main__":
    DataAggregator.run_server()
