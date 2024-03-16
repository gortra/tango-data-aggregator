__all__ = (
    # Classes
    "DataAggregator",
    "RunConfigurator"
)

from .DataAggregator import DataAggregator
from .RunConfigurator import RunConfigurator

import warnings
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("tango-da")
except PackageNotFoundError:
    warnings.warn("Could not find tango-da version")
    __version__ = "0.0.0"
finally:
    del version, PackageNotFoundError
