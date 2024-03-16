Continuous data acquisition using Tango
=======================================

Tango-controls allows various types of data access. The polling mechanism, and the push mechanism. Generally spoken, these are the two fundamentally different types of taking data:

- move, wait, take snapshot (polling model)
- receive data as soon as it is available (push/event model)

The former model generates datasets that are relatively easy to analyze, since the structure is basically given by the data acquisition routine. The second model produces labeled data (timestamps). Analysis of such data requires multidimensional binning routines (binning in its general sense: a grouping step followed by a data reduction step).

The package contains two device-classes that can be used in the context of the latter acquisition type (use sardana for the former). The device class "DA" (data-aggregator) dumps labeled data into hdf5 files. The device class "RunConfigurator" can be used to control several data aggregators at the same time (Start, Stop, Pause, ...). It also handles creation of directories and similar tasks.

## Installation

- Activate dedicated virtual environment
- pip install .

The only direct dependency is PyTango.

## Usage

### Start Tango device-server in console

    cd .tango_da/dserver
    python DA.py <server-instance>
    python RunConfigurator.py <server-instance>

## what is Tango-Controls

(from https://tango-controls.readthedocs.io/en/latest/overview/overview.html)

Tango Controls is a toolkit for building distributed object based control systems. 



