import os
import json
import collections
import time

import tango
import tango.server as ts


# ------------------------------------------------------------------
# helper methods
# ------------------------------------------------------------------
def is_cmd_allowed(device):
    if device.get_state() == tango.DevState.ON:
        return True
    else:
        return False


# ------------------------------------------------------------------
# device class
# ------------------------------------------------------------------
class RunConfigurator(ts.Device):
    green_mode = tango.GreenMode.Asyncio

    # ------------------------------------------------------------------
    # internal variables
    # ------------------------------------------------------------------
    _v_init = ("Attributes", ({"init": ["", "", ""]}))
    _var_run = _v_init
    _var_poll = _v_init
    _var_push = _v_init
    _data_aggregators = {}
    _group_da = tango.Group("data_aggregators")
    _config_file = "./attr_list.json"

    _run_number = 1
    _name_experiment = ""
    _dir_exp = os.path.join("")
    _dir_run = os.path.join("")

    _poll_period = 3.0
    _time_start = 0.0

    # ------------------------------------------------------------------
    # class and device properties
    # ------------------------------------------------------------------
    root_directory = ts.device_property(dtype=str, mandatory=True)
    allowed_experiments = ts.device_property(
        dtype=tango.DevVarStringArray,
        mandatory=True
    )

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    async def init_device(self):
        await super().init_device()
        self.debug_stream(f'initialize device')
        self.set_state(tango.DevState.INIT)

        for exp_name in self.allowed_experiments:
            dir_exp = os.path.join(self.root_directory, exp_name)
            dir_run = os.path.join(self.root_directory, exp_name, "run_1")
            try:
                if not os.path.isdir(dir_exp):
                    os.mkdir(dir_exp)
                    self.debug_stream(f"initialized experiment {exp_name}")
                if not os.path.isdir(dir_run):
                    os.mkdir(dir_run)
                    self.debug_stream("initialized 'run_1'")
            except OSError as err:
                self.debug_stream(f"{err}")
                pass

        self.set_state(tango.DevState.ON)
        self.set_status("device has been initialized")

    # ------------------------------------------------------------------
    # attributes
    # ------------------------------------------------------------------
    @ts.pipe(label="Run Attributes")
    async def attr_run(self):
        return self._var_run

    @ts.pipe(label="Polled Attributes")
    async def attr_poll(self):
        return self._var_poll

    @ts.pipe(label="Pushed Attributes")
    async def attr_push(self):
        return self._var_push

    @ts.attribute(
        label="Name Experiment",
        memorized=True,
        hw_memorized=True,
        dtype=str,
    )
    async def name_experiment(self):
        return self._name_experiment

    @name_experiment.write
    async def name_experiment(self, name_experiment):
        if name_experiment not in self.allowed_experiments:
            raise ValueError(f"Allowed experiments: {self.allowed_experiments}")
        self._name_experiment = name_experiment
        self._dir_exp = os.path.join(self.root_directory,
                                     self._name_experiment)

    @ts.attribute(
        label="Run Number",
        dtype=int,
        min_value=0,
    )
    async def run_number(self):
        return self._run_number

    @ts.attribute(
        label="Config File",
        memorized=True,
        hw_memorized=True,
        dtype=str,
    )
    async def config_file(self):
        return self._config_file

    @config_file.write
    async def config_file(self, config_file):
        self._config_file = config_file

    @ts.attribute(
        label="Polling period",
        memorized=True,
        hw_memorized=True,
        dtype=float,
        unit='s',
    )
    async def polling_period(self):
        return self._poll_period

    @polling_period.write
    async def polling_period(self, polling_period):
        self._poll_period = polling_period

    @ts.attribute(
        label="Run size",
        dtype=float,
        unit="MB",
    )
    async def run_size(self):
        if os.path.isdir(self._dir_run):
            n_bytes = sum(
                d.stat().st_size for d in os.scandir(self._dir_run) if d.is_file()
            )
            return n_bytes / (1024**2)
        else:
            return 0.0

    @ts.attribute(
        label="Run duration",
        dtype=float,
        unit="s",
    )
    async def run_duration(self):
        t_out = 0.0
        if self.get_state() == tango.DevState.RUNNING:
            t_out = (time.time() - self._time_start)
        return t_out

    # ------------------------------------------------------------------
    # commands
    # ------------------------------------------------------------------
    @ts.command(fisallowed=is_cmd_allowed)
    async def InitializeNewRun(self):
        """
        Shortcut for
            - LoadConfiguration()
            - ApplyConfiguration()
        """
        self.debug_stream('Initialize a new run')
        await self._load_config()
        await self._apply_config()

    @ts.command(fisallowed=is_cmd_allowed)
    async def LoadConfiguration(self):
        self.debug_stream('Load configuration file')
        await self._load_config()

    @ts.command(fisallowed=is_cmd_allowed)
    async def ApplyConfiguration(self):
        self.debug_stream('apply run configuration settings')
        await self._apply_config()

    @ts.command(fisallowed=is_cmd_allowed)
    async def StartRecording(self):
        """
        Put all data aggregators into recording mode.

        Notes
        -----
        Will use groups as soon as useful documentation is available
        """
        self.debug_stream('start recording data')

        for dev_da in self._data_aggregators.values():
            try:
                self.debug_stream(f"Try to start device {dev_da.dev_name()}")
                dev_da.Start()
            except tango.DevError as err:
                self.warn_stream(f"{err}")
                self.set_state(tango.DevState.FAULT)
                pass

        self.set_state(tango.DevState.RUNNING)
        self._time_start = time.time()
        self.set_status("start single measurement")

    @ts.command()
    async def StopRecording(self):
        """
        Stop data aggregators

        Notes
        -----
        Will use groups as soon as useful documentation is available
        """
        self.debug_stream('stop recording data')

        for dev_da in self._data_aggregators.values():
            try:
                dev_da.Stop()
            except tango.DevFailed as err:
                self.warn_stream(f"{err}")
                self.set_state(tango.DevState.FAULT)
                pass

        self.set_state(tango.DevState.ON)
        self.set_status("stop single measurement")

    # ------------------------------------------------------------------
    # internal methods
    # ------------------------------------------------------------------
    async def _load_config(self):
        self.set_state(tango.DevState.RUNNING)
        with open(self._config_file) as f:
            data_config = json.load(f)
        self._data_aggregators = {}

        def read_config_file(attr_type):
            attrs = data_config[attr_type]
            d_pipe = collections.OrderedDict()
            for attr in attrs:
                d_pipe[attr[0]] = attr[1::]
                name_da = attr[3]
                if name_da not in self._data_aggregators:
                    self._data_aggregators[name_da] = None
            return d_pipe

        self._var_run = "Attributes", read_config_file("attributes_run")
        self._var_poll = "Attributes", read_config_file("attributes_poll")
        self._var_push = "Attributes", read_config_file("attributes_push")
        self.set_state(tango.DevState.ON)
        self.set_status(f"loaded config file: {self._config_file}")

    async def _apply_config(self):
        self.set_state(tango.DevState.RUNNING)
        # find last existing run and increment by one
        runs = sorted(os.listdir(str(self._dir_exp)))
        dir_run_last = max(runs, key=lambda x: int(x.split("_")[1]))
        run_last = int(dir_run_last.split("_")[1])

        if len(os.listdir(str(self._dir_exp) + "/" + dir_run_last)) == 0:
            self._run_number = run_last
        else:
            self._run_number = run_last + 1
            os.mkdir(self._dir_exp + f"/run_{self._run_number}")

        self._dir_run = os.path.join(
            str(self._dir_exp),
            f"run_{self._run_number}"
        )

        def filter_da_data(dev_name, data_dic):
            data_send = collections.OrderedDict()
            for key in data_dic[1]:
                if dev_name in data_dic[1][key]:
                    data_send[key] = data_dic[1][key]
            return data_send

        for da in self._data_aggregators:
            self._group_da.add(da)
            dev_da = tango.DeviceProxy(da)
            self._data_aggregators[da] = dev_da
            dev_da.attrs_run = (
                "Attributes",
                filter_da_data(da, self._var_run)
            )
            dev_da.attrs_poll = (
                "Attributes",
                filter_da_data(da, self._var_poll)
            )
            dev_da.attrs_push = (
                "Attributes",
                filter_da_data(da, self._var_push)
            )
            dev_da.file_path = self._dir_run
            dev_da.polling_period = self._poll_period

        self.set_state(tango.DevState.ON)
        da_all = self._group_da.get_device_list()
        self.set_status(f"- configured data aggregators {str(da_all)}\n")

if __name__ == "__main__":
    RunConfigurator.run_server()