import pandas as pd
from tqdm import tqdm
from binance.client import Client as binance_client
import datetime
import requests
import threading
import time



#print(info)


class BinanceFuturesClient:
	def __init__(self, public_key, secret_key, testnet):
		if testnet:
			self.base_url = 'https://testnet.binancefuture.com'
		else:
			self.base_url = "https://fapi.binance.com"

		# Instance Variables i.e. universal variables
		self.public_key = public_key
		self.secret_key = secret_key

		self.headers = {'X-MBX-APIKEY': self.public_key}

		self.prices = dict()

	#	def make_signature

	def make_request(self, method, endpoint, data):
		if method == 'GET':
			response = requests.get(self.base_url + endpoint, params=data,
									headers=self.headers)  # params are in a dict

		else:
			raise ValueError()

		if response.status_code == 200:
			return response.json()

		else:

			return 'ERROR'


def funding_bot(days_t,binance_secret,binance_api):


			##Binance - BTCUSD,ETHUSD FUNDING

		binance = BinanceFuturesClient(binance_secret,binance_api, False)

		####startTime for Binance API  - need to essentially consider

		last_timestamp = datetime.datetime.timestamp(
			pd.to_datetime(datetime.date.today() - datetime.timedelta(days=days_t))) * 1000
		last_timestamp_2 = datetime.datetime.timestamp(
			pd.to_datetime(datetime.date.today() - datetime.timedelta(days=100))) * 1000

		##get more funding data from binance
		ticker = ['BTCUSDT', 'ETHUSDT']

		funding_binance_df = pd.DataFrame()
		for i in ticker:
			funding_data_2 = binance.make_request('GET', '/fapi/v1/fundingRate',
												  {'symbol': i, 'limit': 1000, 'startTime': int(last_timestamp_2)})

			date_value = []
			funding_value = []
			for i in range(0, len(funding_data_2)):
				date_value.append(datetime.datetime.utcfromtimestamp(funding_data_2[i]["fundingTime"] / 1000).strftime(
					"%Y-%m-%d %H:%M:%S"))
				funding_value.append(funding_data_2[i]["fundingRate"])

			date_value = pd.DataFrame(date_value, columns=["DATE"])
			funding_value = pd.DataFrame(funding_value, columns=[f'{i}'])
			funding_binance_df_2 = pd.concat([date_value, funding_value], axis=1)
			funding_binance_df_2.index = funding_binance_df_2.DATE
			funding_binance_df_2 = funding_binance_df_2.drop(['DATE'], axis=1)
			funding_binance_df_2[f'{i}'] = (funding_binance_df_2[f'{i}'].astype("float"))

			funding_binance_df = pd.concat([funding_binance_df, funding_binance_df_2], axis=1)
			funding_binance_df[f'{i}'] = (funding_binance_df[f'{i}'].astype(float))

		funding_binance_df.columns = ticker

		###WE WANT: current funding, current ann. funding, last 5 avg, last 10 avg, last 15 avg, last 20 avg, last 30 avg

		btc_eth_funding_stats = pd.DataFrame()
		btc_eth_funding_stats['Curr'] = funding_binance_df.iloc[-1] * 100
		btc_eth_funding_stats['Ann.'] = funding_binance_df.iloc[-1] * 1460 * 100
		btc_eth_funding_stats['T-7'] = ((funding_binance_df.iloc[:-56].mean()) * 1460 * 100)
		btc_eth_funding_stats['(T-7)-7'] = ((funding_binance_df.iloc[-112:-56].mean()) * 1460 * 100)
		btc_eth_funding_stats['((T-7)-7)-7'] = ((funding_binance_df.iloc[-224:-112].mean()) * 1460 * 100)
		btc_eth_funding_stats['(((T-7)-7)-7)-7'] = ((funding_binance_df.iloc[-448:-224].mean()) * 1460 * 100)

		#			btc_eth_funding_stats = btc_eth_funding_stats.round(2)

		###FUNDING FOR ALL TOKENS - THEN WE RANK TOP TO BOTTOM

		client = binance_client()
		info = client.get_exchange_info()

		###We get the list of symbols

		symbols = [x['symbol'] for x in info['symbols']]

		# print(symbols)

		## we exclude leverage tokens

		exclude = ['UP', 'DOWN', 'BEAR', 'BULL']  # exclude leveraged tokens

		non_lev = [symbol for symbol in symbols if all(excludes not in symbol for excludes in exclude)]

		###WE only want USDT assets

		ticker = [symbol for symbol in non_lev if symbol.endswith('USDT')]

		# ticker = ticker[:10]

		##get more funding data from binance

		def get_funding_history(symbol, start_time):
			funding_data_2 = binance.make_request('GET', '/fapi/v1/fundingRate',
												  {'symbol': symbol, 'limit': 1000, 'startTime': start_time})

			df = []

			for i in range(0, len(funding_data_2)):
				date_value = datetime.datetime.utcfromtimestamp(funding_data_2[i]["fundingTime"] / 1000).strftime(
					"%Y-%m-%d %H:%M:%S")
				funding_value = funding_data_2[i]["fundingRate"]
				df.append([date_value, funding_value])

			return df

		klines = {}

		# Create a list to store the thread objects
		threads = []

		delay_between_requests = 0.028  # can adjust this value based on your rate limits

		# Start threads for each symbol
		for symbol in tqdm(ticker):
			thread = threading.Thread(
				target=lambda s=symbol: klines.update({s: get_funding_history(s, start_time=int(last_timestamp_2))}))
			threads.append(thread)
			thread.start()

			time.sleep(delay_between_requests)

		# Wait for all threads to finish
		for thread in threads:
			thread.join()

		####we just drop the 4 hour ones for now

		min_length = min(len(data) for symbol, data in klines.items() if symbol == 'BTCUSDT')

		klines = {symbol: data for symbol, data in klines.items() if len(data) <= min_length}

		funding_binance_df = pd.DataFrame()
		for key, values in klines.items():
			try:
				df = pd.DataFrame(klines[f'{key}'])
				df.columns = ['DATE', f'{key}']
				funding_binance_df = pd.concat([funding_binance_df, df], axis=1)
			except:
				pass


		funding_binance_df = funding_binance_df.dropna(axis=1)

		funding_binance_df = funding_binance_df.loc[:, ~funding_binance_df.columns.duplicated()]

		# funding_binance_df = funding_binance_df.dropna(axis=1)

		funding_binance_df.index = funding_binance_df['DATE']
		funding_binance_df = funding_binance_df.drop(['DATE'], axis=1)

		funding_binance_df = funding_binance_df.astype('float')



		###we rank the funding by highest and lowest
		funding_ranks = funding_binance_df.T

		ranked_funding = funding_ranks.sort_values(by=f'{funding_ranks.columns[-1]}', ascending=False)
		top_10 = ranked_funding.head(15).T
		bottom_10 = ranked_funding.tail(15).T

		top_10_stats = pd.DataFrame()
		top_10_stats['Curr'] = top_10.iloc[-1] * 100
		top_10_stats['Ann.'] = top_10.iloc[-1] * 1095 * 100
		top_10_stats['T-7'] = ((top_10.iloc[:-56].mean(skipna=False)) * 1095 * 100)
		top_10_stats['(T-7)-7'] = ((top_10.iloc[-112:-56].mean(skipna=False)) * 1095 * 100)
		top_10_stats['((T-7)-7)-7'] = ((top_10.iloc[-224:-112].mean(skipna=False)) * 1095 * 100)
		top_10_stats['(((T-7)-7)-7)-7'] = ((top_10.iloc[-448:-224].mean(skipna=False)) * 1095 * 100)


		#			top_10_stats = top_10_stats.round(2)

		bottom_10_stats = pd.DataFrame()
		bottom_10_stats['Curr'] = bottom_10.iloc[-1] * 100
		bottom_10_stats['Ann.'] = bottom_10.iloc[-1] * 1460 * 100
		bottom_10_stats['T-7'] = ((bottom_10.iloc[:-56].mean()) * 1460 * 100)
		bottom_10_stats['(T-7)-7'] = ((bottom_10.iloc[-112:-56].mean()) * 1460 * 100)
		bottom_10_stats['((T-7)-7)-7'] = ((bottom_10.iloc[-224:-112].mean()) * 1460 * 100)
		bottom_10_stats['(((T-7)-7)-7)-7'] = ((bottom_10.iloc[-448:-224].mean()) * 1460 * 100)

		bottom_10_stats = bottom_10_stats.sort_values('Curr', ascending=True)

		return btc_eth_funding_stats,top_10_stats,bottom_10_stats

