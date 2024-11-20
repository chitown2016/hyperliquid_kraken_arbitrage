from hyperliquid.info import Info
from hyperliquid.utils import constants

mainnet_url = constants.MAINNET_API_URL

def get_info(**kwargs):
    base_url = kwargs.get('base_url', mainnet_url)
    skip_ws = kwargs.get('skip_ws', True)
    return Info(base_url, skip_ws)
