import eth_account
from eth_account.signers.local import LocalAccount
import json
import os
from dotenv import find_dotenv, load_dotenv

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

load_dotenv(find_dotenv())


def setup(base_url=None, skip_ws=False):
    
    account: LocalAccount = eth_account.Account.from_key(os.getenv('metamask_secret_key'))

    address = account.address
    print("Running with account address:", address)

    info = Info(base_url, skip_ws)
    user_state = info.user_state(address)
    margin_summary = user_state["marginSummary"]
    if float(margin_summary["accountValue"]) == 0:
        print("Not running the example because the provided account has no equity.")
        url = info.base_url.split(".", 1)[1]
        error_string = f"No accountValue:\nIf you think this is a mistake, make sure that {address} has a balance on {url}.\nIf address shown is your API wallet address, update the config to specify the address of your account, not the address of the API wallet."
        raise Exception(error_string)
    exchange = Exchange(account, base_url, account_address=address)
    return address, info, exchange