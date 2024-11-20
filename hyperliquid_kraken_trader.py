
import ccxt
import telegram as tl
from hyperliquid.utils import constants
import datetime as dt
import os
from dotenv import find_dotenv, load_dotenv
import pytz
import traceback
import example_utils
from enum import Enum
import pandas as pd
import time

load_dotenv(find_dotenv())

api_key = os.getenv('kraken_api_key')
private_key = os.getenv('kraken_private_key')
telegram_token = os.getenv('kraken_hyperliquid_telegram_bot_token')
telegram_chat_id = os.getenv('telegram_chat_id')

# this bot successfully scans the arbitrage opportunities between kraken and hyperliquid.
# remember for kraken you have 0.25% maker and 0.40% taker fees. so if you find an opportunity and immediately execute
# you will give to kraken 0.80%. When I run this code I can see that when the price spread is more than 1%
# these opportunities are fleeting so I don't see an opportunity to make a market to take advantage of these opportunities.
# I can also see that the opportunities are not that frequent at all.


class BotStatus(Enum):
    SCANNING = 1
    QUOTING_2LEGS = 2
    QUOTING_1LEG = 3


class ArbBot():
    def __init__(self):
        self.kraken = ccxt.kraken ({'apiKey': api_key, 'secret': private_key,
        'verbose' : False})
        self.telegram_bot = tl.Bot(token=telegram_token)
        self.telegram_chat_id = telegram_chat_id

        address, info, exchange = example_utils.setup(base_url=constants.MAINNET_API_URL, skip_ws=True)
        self.hyperliquid_info = info
        self.hyperliquid_exchange = exchange
        self.current_opportunity_list = []
        self.num_max_opportunities = 1

    def get_common_symbols(self):

        all_mids = self.hyperliquid_info.all_mids()
        hyperliquid_symbols = all_mids.keys()
        kraken_markets = self.kraken.load_markets()
        kraken_symbols = kraken_markets.keys()

        common_symbols = []

        for symbol in hyperliquid_symbols:
            if symbol + '/USD' in kraken_symbols:
                common_symbols.append(symbol)

        return common_symbols
    
    def get_hyperliquid_book_data(self,**kwargs):

        symbol = kwargs['symbol']

        snapshot_output = self.hyperliquid_info.l2_snapshot(symbol)

        if len(snapshot_output['levels'][0])==0 or len(snapshot_output['levels'][1])==0:
            return {'success': False, 'bid': pd.DataFrame(), 'ask': pd.DataFrame()}

        bid_frame = pd.DataFrame(snapshot_output['levels'][0])
        ask_frame = pd.DataFrame(snapshot_output['levels'][1])

        bid_frame['price'] = bid_frame['px'].astype('float64')
        bid_frame['quantity'] = bid_frame['sz'].astype('float64')
        
        bid_frame.drop(['px', 'sz', 'n'], axis=1, inplace=True)

        ask_frame['price'] = ask_frame['px'].astype('float64')
        ask_frame['quantity'] = ask_frame['sz'].astype('float64')
        ask_frame.drop(['px', 'sz', 'n'], axis=1, inplace=True)

        return {'success': True, 'bid': bid_frame, 'ask': ask_frame} 

    def get_kraken_book_data(self,**kwargs):

        symbol = kwargs['symbol'] + '/USD'

        order_book_output = self.kraken.fetchL2OrderBook(symbol)

        bid_frame = pd.DataFrame(order_book_output['bids'], columns=['price', 'quantity'])  
        ask_frame = pd.DataFrame(order_book_output['asks'], columns=['price', 'quantity'])

        return {'bid': bid_frame, 'ask': ask_frame}

    def calculate_immediate_execution_price(self,**kwargs):

        book_data = kwargs['book_data']
        target_dollar_volume = kwargs['target_dollar_volume']

        book_data['pq'] = book_data.price*book_data.quantity
        book_data['pq_cum'] = book_data['pq'].cumsum()

        aux_data = book_data[book_data['pq_cum']<target_dollar_volume]

        if len(aux_data) == 0:
            # simply return the first level price if the quoted volume is big enough
            return book_data.price.iloc[0]
        else:
            select_data = book_data.iloc[:(len(aux_data)+1)].copy()

            select_data['pq'].iloc[-1] = target_dollar_volume - select_data['pq'].iloc[:-1].sum()
            select_data['quantity'].iloc[-1] = select_data['pq'].iloc[-1]/select_data['price'].iloc[-1]

            return target_dollar_volume/select_data['quantity'].sum()
    
    def scan_4_opportunities(self):

        common_symbols = self.get_common_symbols()
        self.current_opportunity_list = []

        for symbol in common_symbols:

            hyperliquid_output = self.get_hyperliquid_book_data(symbol=symbol)

            if not hyperliquid_output['success']:
                continue

            hyperliquid_mid_price = (hyperliquid_output['bid']['price'].iloc[0] + hyperliquid_output['ask']['price'].iloc[0])/2

            

            kraken_output = self.get_kraken_book_data(symbol=symbol)
            kraken_mid_price = (kraken_output['bid']['price'].iloc[0] + kraken_output['ask']['price'].iloc[0])/2

            mid_price_opportunity = round(100*(hyperliquid_mid_price-kraken_mid_price)/kraken_mid_price,1)

            if mid_price_opportunity>1:
                hyperliquid_immediate_price_1000 = self.calculate_immediate_execution_price(book_data=hyperliquid_output['bid'], target_dollar_volume=1000)
                hyperliquid_immediate_price_10000 = self.calculate_immediate_execution_price(book_data=hyperliquid_output['bid'], target_dollar_volume=10000)

                kraken_immediate_price_1000 = self.calculate_immediate_execution_price(book_data=kraken_output['ask'], target_dollar_volume=1000)
                kraken_immediate_price_10000 = self.calculate_immediate_execution_price(book_data=kraken_output['ask'], target_dollar_volume=10000)

                immediate_opportunity_1000 = 100*(hyperliquid_immediate_price_1000-kraken_immediate_price_1000)/kraken_immediate_price_1000
                immediate_opportunity_10000 = 100*(hyperliquid_immediate_price_10000-kraken_immediate_price_10000)/kraken_immediate_price_10000


                opportunity_i = {"symbol": symbol, "mid_price_opportunity": mid_price_opportunity,
                                 "immediate_opportunity_1000": round(immediate_opportunity_1000,1),
                                 "immediate_opportunity_10000": round(immediate_opportunity_10000,1)}
                self.current_opportunity_list.append(opportunity_i)

            #self.bot.send_message(chat_id=self.telegram_chat_id, text=f"Mid price opportunity for {['A','B']} is: {mid_price_opportunity}")
            time.sleep(1)

        if len(self.current_opportunity_list)>0:
            for opportunity in self.current_opportunity_list:
                text_string = (
                               f"{opportunity['symbol']} opportunity detected with "
                               f"{opportunity['mid_price_opportunity']}, {opportunity['immediate_opportunity_1000']}, {opportunity['immediate_opportunity_10000']}"
                               )
                self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=text_string)
            


if __name__ == '__main__':
    my_bot = ArbBot()

    while True:

        try:
            my_bot.scan_4_opportunities()
            print(f"Last scan finished on: {dt.datetime.now(pytz.UTC)}")
            # if len(my_bot.current_opportunity_list)>0:
            #     symbol_i = my_bot.current_opportunity_list[0]

            #     hyperliquid_output = my_bot.get_hyperliquid_book_data(symbol=symbol_i)

            #     if not hyperliquid_output['success']:
            #         continue

            #     hyperliquid_mid_price = (hyperliquid_output['bid']['price'].iloc[0] + hyperliquid_output['ask']['price'].iloc[0])/2

            #     kraken_output = my_bot.get_kraken_book_data(symbol=symbol_i)
            #     kraken_mid_price = (kraken_output['bid']['price'].iloc[0] + kraken_output['ask']['price'].iloc[0])/2
        except Exception as e:
            error_message = traceback.format_exc()
            print(f"Kraken Hyperliquid Arb Bot Error: {error_message}")
            print(f"Exception type: {e.__class__}")

            my_bot.telegram_bot.send_message(chat_id=my_bot.telegram_chat_id, text=f"Kraken Hyperliquid Arb Bot Error: {error_message[:3900]}")
            time.sleep(20)

    #order_result = my_bot.hyperliquid_exchange.order("ETH", True, 0.01, 1100, {"limit": {"tif": "Gtc"}})
    #print(common_symbols)
