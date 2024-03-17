Continuous data acquisition using Tango
=======================================

## Overview
Generally spoken, these are the two fundamentally different types of taking data:

- polling: move, wait, take snapshot, repeat
- event driven: receive data as soon as it is available

The former mechanism generates datasets that are relatively easy to analyze, since the structure is given by the data acquisition routine. There is an identical dimension to each data field in the dataset. Event driven acquisition on the other hand, produces labeled (timestamps) datasets. The Analysis of such data typically requires multidimensional binning routines.

Tango-controls allows both types of data access. The polling mechanism, and the push mechanism. The core of the present package is a device class ("DataAggregator") that tracks a set of tango-attributes using any of the two aforementioned schemes. Another device class ("RunConfigurator") may be used to control a group of DataAggregators collectively. The package is inspired by the data acquisition scheme deployed in Karabo at the EuropeanXFEL.

## Installation

- Activate dedicated virtual environment
- pip install .

Dependencies are: PyTango, h5py and numpy

## Usage

### 1: Start Tango device servers

    cd .tango_da/dserver
    python DA.py <registered-server-instance>
    python RunConfigurator.py <registered-server-instance>

### 2: Prepare a config file

The Run Configurator reads a configuration file in json format that contains information about how the data should be tracked. A minimal config file might have the following entries:

    {
       "attributes_run": [
    
          [
             "status",
             "sys/tg_test/1",
             "status",
             "<data-aggregator-x>"
          ]
    
       ],
    
      "attributes_poll": [
    
          [
             "spectrum-int",
             "sys/tg_test/1",
             "long_spectrum_ro",
             "<data-aggregator-y>"
          ]
    
       ],
    
      "attributes_push": [
    
          [
             "wave",
             "sys/tg_test/1",
             "wave",
             "<data-aggregator-x>"
          ]
    
      ]
    }

All attributes in the group "attributes_run" are being stored once when a measurement is being started. This is mainly used for static metadata like serial numbers of devices for example. Attributes in the group "attributes_poll" are being polled once per poll period. All attributes in the group "attributes_push" are expected to generate change events. The corresponding data aggregator subscribes to such events and stores all data it receives. In the example above, a device "<data-aggregator-x" would track the attribute "wave" of the device "sys/tg_test/1". When hundreds, or even thousands of attributes are being tracked, it makes sense to distribute the workload to several data aggregators.

### 3. Use the run configurator to initialize and start all data aggregators

As a user it is not necessary to interact with a data-aggregator device. Use the run configurator instead. A run configurator has two mandatory device properties that have to be set before instantiation. The administrator has to define a root directory in which all data is being stored. Additionally, the administrator has to define a set of allowed names for experiments. A device of class RunConfigurator can then only be used to store runs within the context of certain experiments.

Once the device is running, the user has to enter the name of an allowed experiment as well as a config file. Additionally, a meaningful poll period "for poll attributes" can be selected. The typical workflow is:

1. InitializeNewRun() -> loads the config file and sends the corresponding information to each data aggregator. The run number is incremented by one.
2. StartRecording() -> All data aggregators start recording data
3. StopRecording() -> All data aggregators stop recording

### Additional information

- Keep in mind python's global interpreter lock! While it is possible to run several devices on the same device-server, they are not actually running in parallel. If the processing time per cycle becomes too high, try running the data-aggregator devices on separate device servers. However, the typical limitation is network bandwidth rather than processing time.
- 


## what is Tango-Controls

(from https://tango-controls.readthedocs.io/en/latest/overview/overview.html)

Tango Controls is a toolkit for building distributed object based control systems. 



