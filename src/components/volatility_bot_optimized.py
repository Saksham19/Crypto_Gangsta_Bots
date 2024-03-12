import pandas as pd
from tqdm import tqdm
from binance.client import Client as binance_client
import numpy as np
import requests
import threading
import time


binance_client = binance_client()
info = binance_client.get_exchange_info()


def market_vol_bot(duration):

		url = 'https://stablecoins.llama.fi/stablecoins?includePrices=true'
		response = requests.get(url)
		response = response.json()

		stable_list = []

		for i in range(len(response['peggedAssets'])):
			stable_list.append(response['peggedAssets'][i]['symbol'] + 'USDT')

		stable_list.append('GBPUSDT')
		stable_list.append('EURUSDT')

		# First, just get btc/eth returns
		from binance.client import Client

		client = Client()
		info = client.get_exchange_info()

		btc_eth = ['BTCUSDT', 'ETHUSDT']

		btc_eth_data = {}

		for symbol in tqdm(btc_eth):  # tqdm - is the progress bar
			btc_eth_data[symbol] = binance_client.get_historical_klines(symbol, '1h',
																		f'{duration}')  # can do 30 mins/45 mins/days etc

		btc_eth_vol_df, btc_eth_upside_vol_df, btc_eth_downside_vol_df, btc_eth_symbols = [], [], [], []

		for symbol in btc_eth:
			if len(btc_eth_data[symbol]) > 0:
				returns = np.log(pd.DataFrame(btc_eth_data[symbol])[4].astype(float).pct_change() + 1)
				volatility_symbol = (returns).std() * np.sqrt(365)
				upside_vol = (returns[returns >= 0]).std() * np.sqrt(365)
				downside_vol = (returns[returns < 0]).std() * np.sqrt(365)
				btc_eth_vol_df.append(volatility_symbol)
				btc_eth_upside_vol_df.append(upside_vol)
				btc_eth_downside_vol_df.append(downside_vol)
				btc_eth_symbols.append(symbol)

		btc_eth_vol_df = pd.DataFrame(btc_eth_vol_df, index=btc_eth_symbols, columns=['Vol %'])
		btc_eth_upside_vol_df = pd.DataFrame(btc_eth_upside_vol_df, index=btc_eth_symbols, columns=['Upside Vol %'])
		btc_eth_downside_vol_df = pd.DataFrame(btc_eth_downside_vol_df, index=btc_eth_symbols, columns=['Downside Vol %'])
		btc_eth_vol_df = pd.concat([btc_eth_vol_df, btc_eth_upside_vol_df, btc_eth_downside_vol_df], axis=1)
		btc_eth_vol_df = btc_eth_vol_df * 100

		############################################################Winners/Losers

		###We get the list of symbols

		symbols = [x['symbol'] for x in info['symbols']]

		# print(symbols)

		## we exclude leverage tokens

		exclude = ['UP', 'DOWN', 'BEAR', 'BULL']  # exclude leveraged tokens

		non_lev = [symbol for symbol in symbols if all(excludes not in symbol for excludes in exclude)]

		###WE only want USDT assets

		relevant = [symbol for symbol in non_lev if symbol.endswith('USDT')]

		relevant = [x for x in relevant if x not in stable_list]


		def get_history(symbol, duration=f'{duration}', granularity='1h'):
			try:
				df = binance_client.get_historical_klines(symbol, granularity, f'{duration}')
				if df is not None:
					return df
				else:
					print(f"Data for symbol {symbol} is None.")
					return None
			except Exception as e:
				print(f"An unexpected error occurred for symbol {symbol}: {e}")
				return None

		# Create a dictionary to store the results for each symbol
		klines = {}

		# Create a list to store the thread objects
		threads = []

		delay_between_requests = 0.028  # You can adjust this value based on your rate limits

		# Start threads for each symbol
		for symbol in tqdm(relevant):
			thread = threading.Thread(target=lambda s=symbol: klines.update({s: get_history(s)}))
			threads.append(thread)
			thread.start()

			time.sleep(delay_between_requests)

		# Wait for all threads to finish
		for thread in threads:
			thread.join()

			# Now, klines contains the data for all relevant symbols

		vol_df, upside_vol_df, downside_vol_df, upside_downside_ratio_df, symbols = [], [], [], [], []

		for symbol in relevant:
			if len(klines[symbol]) > 0:
				returns = np.log(pd.DataFrame(klines[symbol])[4].astype(float).pct_change() + 1)
				volatility_symbol = (returns).std() * np.sqrt(365)
				upside_vol = returns[returns >= 0].std() * np.sqrt(365)
				downside_vol = returns[returns < 0].std() * np.sqrt(365)
				upside_downside_ratio = (upside_vol / downside_vol) / 100
				vol_df.append(volatility_symbol)
				upside_vol_df.append(upside_vol)
				downside_vol_df.append(downside_vol)
				upside_downside_ratio_df.append(upside_downside_ratio)
				symbols.append(symbol)

		vol_df = pd.DataFrame(vol_df, index=symbols, columns=['Vol %'])
		upside_vol_df = pd.DataFrame(upside_vol_df, index=symbols, columns=['Upside Vol %'])
		downside_vol_df = pd.DataFrame(downside_vol_df, index=symbols, columns=['Downside Vol %'])
		upside_downside_ratio_df = pd.DataFrame(upside_downside_ratio_df, index=symbols, columns=['Upside/Downside Vol %'])

		combined_df = pd.concat([vol_df, upside_vol_df, downside_vol_df, upside_downside_ratio_df], axis=1).round(4)
		combined_df = combined_df.dropna(subset=['Vol %'])
		combined_df = combined_df.sort_values(by=['Vol %'], ascending=False)
		combined_df = combined_df * 100

		combined_df_top_10 = combined_df.head(10)

		combined_df_bottom_10 = combined_df.tail(10)
		combined_df_bottom_10 = combined_df_bottom_10.sort_values(by=['Vol %'], ascending=True)

		print(combined_df_top_10)
		print(combined_df_bottom_10)

		return btc_eth_vol_df,combined_df_top_10,combined_df_bottom_10

