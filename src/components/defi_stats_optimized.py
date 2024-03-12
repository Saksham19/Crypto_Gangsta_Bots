import pandas as pd
from tqdm import tqdm
from binance.client import Client as binance_client
import datetime
from datetime import timedelta
import discord
import requests
import threading
import time
import numpy as np


#print(info)


def defi_stats_bot(days_t):

		res = requests.get('https://api.llama.fi/chains')
		res = res.json()

		chain_names = []
		for i in range(len(res)):
			if res[i]['name'] is not None:
				chain_names.append(res[i]['name'])

		###GET THE TVL BY CHAIN


		#chain_names = chain_names[:5]



		def get_chains_tvl_data(chain_name):


			res = requests.get(f'https://api.llama.fi/charts/{chain_name}')
			res = res.json()
			df = []


			try:
				for j in range(len(res)):
					df.append(res[j]['totalLiquidityUSD'])
			except:
				pass


			return df




		klines = {}

		# Create a list to store the thread objects
		threads = []

		delay_between_requests = 0.00  # can adjust this value based on your rate limits

		# Start threads for each symbol
		for symbol in tqdm(chain_names):
			thread = threading.Thread(target=lambda s=symbol: klines.update({s: get_chains_tvl_data(s)}))
			threads.append(thread)
			thread.start()

			time.sleep(delay_between_requests)

		# Wait for all threads to finish
		for thread in threads:
			thread.join()





		###RANK BY CHANGE


		##1D Chaange
		change_1d = []
		change_3d = []
		change_7d = []
		change_14d = []
		change_30d = []
		change_90d = []


		for key in klines:
			try:
				change_1d.append(((klines[key][-1]/klines[key][-2])-1)*100)
			except:
				change_1d.append(np.nan)
			try:
				change_3d.append(((klines[key][-1]/klines[key][-4])-1)*100)
			except:
				change_3d.append(np.nan)
			try:
				change_7d.append(((klines[key][-1]/klines[key][-8])-1)*100)
			except:
				change_7d.append(np.nan)
			try:
				change_14d.append(((klines[key][-1]/klines[key][-15])-1)*100)
			except:
				change_14d.append(np.nan)
			try:
				change_30d.append(((klines[key][-1]/klines[key][-31])-1)*100)
			except:
				change_30d.append(np.nan)
			try:
				change_90d.append(((klines[key][-1]/klines[key][-91])-1)*100)
			except:
				change_90d.append(np.nan)

		tvl_change_df = pd.concat([pd.DataFrame(change_1d),pd.DataFrame(change_3d),pd.DataFrame(change_7d),pd.DataFrame(change_14d),pd.DataFrame(change_30d),pd.DataFrame(change_90d)],axis=1)
		tvl_change_df = tvl_change_df.T
		tvl_change_df.columns = chain_names
		tvl_change_df.index = ['1D %','3D %','7D %','14D %','30D %','90D %']
		tvl_change_df = tvl_change_df.T


		tvl_change_df = tvl_change_df.round(2)
		print(tvl_change_df)
		###we can basically rank the tvl's based on user input - if want 1d - then rank by 1d - if want 3d - rank by 3d etc.

		tvl_change_df = tvl_change_df.dropna(subset=[f'{days_t}D %'])
		tvl_change_df_sorted = tvl_change_df.sort_values(f'{days_t}D %',ascending=False)  ###can change this based on what timeframe we looking at


		##Grab the top 20 and bottom 20?
		tvl_change_df_top_10  = tvl_change_df_sorted.iloc[:10]
		tvl_change_df_top_20  = tvl_change_df_sorted.iloc[10:20]
		tvl_change_df_top_30  = tvl_change_df_sorted.iloc[20:30]


		tvl_change_df_bottom_10 = tvl_change_df_sorted.tail(10)
		tvl_change_df_bottom_10 = tvl_change_df_bottom_10.sort_values(f'{days_t}D %',ascending=False)

		print(tvl_change_df_top_10)

		print(tvl_change_df_bottom_10)


		####vol stuff here

		chain_name_new = []


		def get_chains_vol_data(chain_name):


			res = requests.get(f'https://api.llama.fi/overview/dexs/{chain_name}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyVolume')
			res = res.json()
			df = []


			try:
				for j in range(len(res['totalDataChart'])):
					df.append(res['totalDataChart'][j][1])
			except:
				pass


			return df

		klines = {}

		# Create a list to store the thread objects
		threads = []

		delay_between_requests = 0.00  # can adjust this value based on your rate limits

		# Start threads for each symbol
		for symbol in tqdm(chain_names):
			def thread_function(symbol):
				try:
					data = get_chains_vol_data(symbol)
					klines.update({symbol: data})
					chain_name_new.append(symbol)
				except Exception as e:
					print(f"Error fetching data for {symbol}: {str(e)}")

			thread = threading.Thread(target=thread_function, args=(symbol,))
			threads.append(thread)
			thread.start()

			time.sleep(delay_between_requests)

		# Wait for all threads to finish
		for thread in threads:
			thread.join()


		##1D Chaange
		change_1d = []
		change_3d = []
		change_7d = []
		change_14d = []
		change_30d = []
		change_90d = []


		for key in klines:
			try:
				change_1d.append(((klines[key][-1]/klines[key][-2])-1)*100)
			except:
				change_1d.append(np.nan)
			try:
				change_3d.append(((klines[key][-1]/klines[key][-4])-1)*100)
			except:
				change_3d.append(np.nan)
			try:
				change_7d.append(((klines[key][-1]/klines[key][-8])-1)*100)
			except:
				change_7d.append(np.nan)
			try:
				change_14d.append(((klines[key][-1]/klines[key][-15])-1)*100)
			except:
				change_14d.append(np.nan)
			try:
				change_30d.append(((klines[key][-1]/klines[key][-31])-1)*100)
			except:
				change_30d.append(np.nan)
			try:
				change_90d.append(((klines[key][-1]/klines[key][-91])-1)*100)
			except:
				change_90d.append(np.nan)

		vol_change_df = pd.concat([pd.DataFrame(change_1d),pd.DataFrame(change_3d),pd.DataFrame(change_7d),pd.DataFrame(change_14d),pd.DataFrame(change_30d),pd.DataFrame(change_90d)],axis=1)
		vol_change_df = vol_change_df.T
		vol_change_df.columns = chain_name_new
		vol_change_df.index = ['1D %','3D %','7D %','14D %','30D %','90D %']
		vol_change_df = vol_change_df.T


		vol_change_df = vol_change_df.round(2)
		print(vol_change_df)


		###we can basically rank the tvl's based on user input - if want 1d - then rank by 1d - if want 3d - rank by 3d etc.

		vol_change_df = vol_change_df.dropna(subset=[f'{days_t}D %'])
		vol_change_df_sorted = vol_change_df.sort_values(f'{days_t}D %',ascending=False)  ###can change this based on what timeframe we looking at


		##Grab the top 20 and bottom 20?
		vol_change_df_top_10  = vol_change_df_sorted.iloc[:10]
		vol_change_df_top_20  = vol_change_df_sorted.iloc[10:20]
		vol_change_df_top_30  = vol_change_df_sorted.iloc[20:30]


		vol_change_df_bottom_10 = vol_change_df_sorted.tail(10)
		vol_change_df_bottom_10 = vol_change_df_bottom_10.sort_values(f'{days_t}D %',ascending=False)


		return tvl_change_df_top_10,tvl_change_df_top_20,tvl_change_df_top_30,tvl_change_df_bottom_10,vol_change_df_top_10,vol_change_df_top_20,vol_change_df_top_30,vol_change_df_bottom_10




