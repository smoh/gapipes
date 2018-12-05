from .core import Tap, GaiaTapPlus

gaia = GaiaTapPlus.from_url(
    "https://gea.esac.esa.int/tap-server/tap",
    server_context='tap-server',
    upload_context='Upload')

__all__ = ['Tap', 'GaiaTapPlus', 'gaia']