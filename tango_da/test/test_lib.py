import pytest
import logging
import tango_da as tda

logging.basicConfig(level=logging.DEBUG)
log_root = logging.getLogger(__name__)


class TestGlobal:
    def test_package(self):
        log_root.info("testing package")
        assert tda.__version__ is not "0.0.0"


