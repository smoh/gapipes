import pytest
from gapipes.gaia.core import Tap


class TestTap(object):

    def test_from_url(self):
        tap = Tap.from_url("http://gea.esac.esa.int:80/tap-server/tap")
        assert tap.protocol == 'http', "Tap has a wrong protocol"
        assert tap.host == 'gea.esac.esa.int', "Tap has a wrong host"
        assert tap.port == 80, "Tap has a wrong port"
        assert tap.path == '/tap-server/tap', "Tap has a wrong path"
        assert tap.tap_endpoint == "http://gea.esac.esa.int/tap-server/tap"

        tap = Tap.from_url("https://gea.esac.esa.int/tap-server/tap")
        assert tap.protocol == 'https', "Tap has a wrong protocol"
        assert tap.port == 443, "Tap has a wrong port"
    