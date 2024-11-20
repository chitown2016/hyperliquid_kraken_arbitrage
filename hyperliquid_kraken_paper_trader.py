# this script runs continuously to scan for arbitrage opportunities between 
# kraken and hyperliquid. The results are saved in pickle files to be analyzed later.


import hyperliquid_api as ha
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import traceback
from dotenv import find_dotenv, load_dotenv
import os
import time 
import ccxt
import telegram as tl

load_dotenv(find_dotenv())

telegram_token = os.getenv('kraken_hyperliquid_telegram_bot_token')
telegram_chat_id = os.getenv('telegram_chat_id')


class ArbBot():
    def __init__(self):
        # instance attributes
        self.hyperliquid_info = ha.get_info()
        self.kraken = ccxt.kraken() 
        self.bot = tl.Bot(token=telegram_token)
        self.telegram_chat_id = telegram_chat_id
    
    def restart_connections(self):
        self.bot.send_message(chat_id=self.telegram_chat_id, text="Attempting to reconnect...")
        time.sleep(10)
        self.kraken = ccxt.kraken() 
        self.hyperliquid_info = ha.get_info()

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


    def generate_opportunity_dictionary(self,**kwargs):

        symbol = kwargs['symbol']
        target_dollar_volume = kwargs.get('target_dollar_volume', 10000)

        datetime_now = datetime.now(timezone.utc)
        #print(datetime_now)

        hyperliquid_output = self.get_hyperliquid_book_data(symbol=symbol)

        output_dictionary = {'symbol': symbol, 'hyperliquid_mid_price': np.nan, 'hyperliquid_immediate_price': np.nan, 
                'kraken_immediate_price': np.nan, 'kraken_mid_price': np.nan,
                'mid_price_opportunity': np.nan, 'immediate_opportunity': np.nan,
                'utc_datetime': datetime.now(timezone.utc)}

        if not hyperliquid_output['success']:
            return output_dictionary

        hyperliquid_mid_price = (hyperliquid_output['bid']['price'].iloc[0] + hyperliquid_output['ask']['price'].iloc[0])/2
    
        hyperliquid_immediate_price = self.calculate_immediate_execution_price(book_data=hyperliquid_output['bid'], target_dollar_volume=target_dollar_volume)

        kraken_output = self.get_kraken_book_data(symbol=symbol)
        kraken_mid_price = (kraken_output['bid']['price'].iloc[0] + kraken_output['ask']['price'].iloc[0])/2

        kraken_immediate_price = self.calculate_immediate_execution_price(book_data=kraken_output['ask'], target_dollar_volume=target_dollar_volume)

        mid_price_opportunity = 100*(hyperliquid_mid_price-kraken_mid_price)/kraken_mid_price
        immediate_opportunity = 100*(hyperliquid_immediate_price-kraken_immediate_price)/kraken_immediate_price

        return {'symbol': symbol, 'hyperliquid_mid_price': hyperliquid_mid_price, 'hyperliquid_immediate_price': hyperliquid_immediate_price, 
                'kraken_immediate_price': kraken_immediate_price, 'kraken_mid_price': kraken_mid_price,
                'mid_price_opportunity': mid_price_opportunity, 'immediate_opportunity': immediate_opportunity,
                'utc_datetime': datetime.now(timezone.utc)}

    def generate_opportunity_dataframe(self,**kwargs):

        common_symbols = self.get_common_symbols()

        opportunity_dictionary_list = []

        for symbol in common_symbols:
            #print(symbol)
            opportunity_dictionary = self.generate_opportunity_dictionary(symbol=symbol, **kwargs)
            opportunity_dictionary_list.append(opportunity_dictionary)
            time.sleep(1)

        return pd.DataFrame(opportunity_dictionary_list)



    def save_opportunty_frames(self,**kwargs):


        while True:

            try:
                datetime_now = datetime.now(timezone.utc)

                file_name = datetime_now.strftime("%Y-%m-%d-%H")

                file_path = os.path.join('output',file_name + ".pkl")
                
                opportunity_frame = self.generate_opportunity_dataframe()

                if os.path.isfile(file_path):
                    old_data = pd.read_pickle(file_path)
                    new_data = pd.concat([old_data, opportunity_frame])
                    new_data.to_pickle(file_path)
                else:
                    opportunity_frame.to_pickle(file_path)
            except Exception as e:
                error_message = traceback.format_exc()
                print(f"Kraken Hyperliquid Arb Bot Error: {error_message}")
                print(f"Exception type: {e.__class__}")

                self.bot.send_message(chat_id=self.telegram_chat_id, text=f"Kraken Hyperliquid Arb Bot Error: {error_message[:3900]}")
                time.sleep(20)
            
if __name__ == '__main__':
    my_bot = ArbBot()
    my_bot.save_opportunty_frames()







    







