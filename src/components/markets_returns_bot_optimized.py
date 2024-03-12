import pandas as pd
from tqdm import tqdm
from binance.client import Client as binance_client
import numpy as np
import datetime
from datetime import timedelta
import threading
import time


binance_client = binance_client()

info = binance_client.get_exchange_info()
#print(info)


def market_returns_bot(duration):

	#First, just get btc/eth returns

	btc_eth = ['BTCUSDT','ETHUSDT']

	btc_eth_data = {}

	for symbol in tqdm(btc_eth):  #tqdm - is the progress bar
		btc_eth_data[symbol] = binance_client.get_historical_klines(symbol,'1h',f'{duration}') #can do 30 mins/45 mins/days etc


	btc_eth_cumret_df, btc_eth_symbols = [],[]

	for symbol in btc_eth:
		if len(btc_eth_data[symbol])>0:
			cumret = (pd.DataFrame(btc_eth_data[symbol])[4].astype(float).pct_change()+1).prod()-1
			btc_eth_cumret_df.append(cumret)
			btc_eth_symbols.append(symbol)


	btc_eth_cumret_df = pd.DataFrame(btc_eth_cumret_df,index=btc_eth_symbols,columns=['cumret'])
	btc_eth_cumret_df = btc_eth_cumret_df*100



	############################################################Winners/Losers####################################

	info = binance_client.get_exchange_info()

	symbols = [x['symbol'] for x in info['symbols']]

	#we exclude leverage tokens
	exclude = ['UP','DOWN','BEAR','BULL'] #exclude leveraged tokens
	non_lev = [symbol for symbol in symbols if all(excludes not in symbol for
	excludes in exclude)]

	#WE only want USDT assets
	relevant = [symbol for symbol in non_lev if symbol.endswith('USDT')]



	# List of relevant symbols

	#relevant = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']  # Replace with your relevant symbols

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


	ret_df,cumret_df, symbols = [],[],[]




	for symbol in relevant:
		try:
			if len(klines[symbol])>0:
				ret = pd.DataFrame(klines[symbol])[4].astype(float).pct_change()
				cumret = (pd.DataFrame(klines[symbol])[4].astype(float).pct_change()+1).prod()-1
				ret_df.append(ret)
				cumret_df.append(cumret)
				symbols.append(symbol)
		except:
			pass

	cumret_df = pd.DataFrame(cumret_df,index=symbols,columns=['cumret'])
	ret_df = pd.DataFrame(ret_df).T
	ret_df.columns = symbols
	ret_df = ret_df.dropna()




	# ##Getting the corr to btc and eth

	corr_val = ret_df.corr()
	corr_btc = corr_val['BTCUSDT']
	corr_eth = corr_val['ETHUSDT']



	# ###Getting the beta to BTC and ETH


	w_var = ret_df.var() #variances of all columns grouped by year
	w_cov = ret_df.cov() #covariances of all columns by year
	beta_var = w_cov / w_var




	btc_beta = beta_var['BTCUSDT']
	eth_beta = beta_var['ETHUSDT']


	###Merge all of these together
	combined_df = cumret_df*100
	combined_df = pd.concat([cumret_df*100,corr_btc],axis=1)
	combined_df = pd.concat([combined_df,corr_eth],axis=1)
	combined_df = pd.concat([combined_df,btc_beta],axis=1)
	combined_df = pd.concat([combined_df,eth_beta],axis=1)

	combined_df.columns = ['Return','BTC Corr','ETH Corr','BTC Beta','ETH Beta']
	#			combined_df.columns = ['Return']
	combined_df = combined_df.round(2)





	combined_df = combined_df.sort_values(by=['Return'],ascending=False)


	combined_df_top_10 = combined_df.head(10)

	combined_df_bottom_10 = combined_df.tail(10)
	combined_df_bottom_10 = combined_df_bottom_10.sort_values(by=['Return'],ascending=True)




	####we want 1D/2D/7D/30D/90D Returns
	final_top_10_symbols = combined_df_top_10.index
	final_bottom_10_symbols = combined_df_bottom_10.index


	one_day_returns=[]
	three_day_returns=[]
	seven_day_returns=[]
	thirty_day_returns=[]
	ninety_day_returns=[]

	for i in final_top_10_symbols:
		price_data = binance_client.get_historical_klines(f'{i}','1h',f'{datetime.datetime.now()-timedelta(days=120)}')
		one_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-25][4]))-1)*100,2))
		try:
			three_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-73][4]))-1)*100,2))
		except:
			three_day_returns.append(np.nan)
		try:
			seven_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-169][4]))-1)*100,2))
		except:
			seven_day_returns.append(np.nan)
		try:
			thirty_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-721][4]))-1)*100,2))
		except:
			thirty_day_returns.append(np.nan)
		try:
			ninety_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-2161][4]))-1)*100,2))
		except:
			ninety_day_returns.append(np.nan)

	one_day_returns = pd.DataFrame(one_day_returns)
	one_day_returns.index = final_top_10_symbols
	one_day_returns['Asset'] = one_day_returns.index
	one_day_returns.columns = ['1D','Asset']

	three_day_returns = pd.DataFrame(three_day_returns)
	three_day_returns.index = final_top_10_symbols
	three_day_returns['Asset'] = three_day_returns.index
	three_day_returns.columns = ['3D','Asset']


	seven_day_returns = pd.DataFrame(seven_day_returns)
	seven_day_returns.index = final_top_10_symbols
	seven_day_returns['Asset'] = seven_day_returns.index
	seven_day_returns.columns = ['7D','Asset']


	thirty_day_returns = pd.DataFrame(thirty_day_returns)
	thirty_day_returns.index = final_top_10_symbols
	thirty_day_returns['Asset'] = thirty_day_returns.index
	thirty_day_returns.columns = ['30D','Asset']


	ninety_day_returns = pd.DataFrame(ninety_day_returns)
	ninety_day_returns.index = final_top_10_symbols
	ninety_day_returns['Asset'] = ninety_day_returns.index
	ninety_day_returns.columns = ['90D','Asset']



	combined_df_top_10['Asset'] = combined_df_top_10.index
	combined_df_top_10 = pd.merge(combined_df_top_10,one_day_returns,on='Asset')
	combined_df_top_10 = pd.merge(combined_df_top_10,three_day_returns,on='Asset')
	combined_df_top_10 = pd.merge(combined_df_top_10,seven_day_returns,on='Asset')
	combined_df_top_10 = pd.merge(combined_df_top_10,thirty_day_returns,on='Asset')
	combined_df_top_10 = pd.merge(combined_df_top_10,ninety_day_returns,on='Asset')

	combined_df_top_10.index = combined_df_top_10['Asset']
	combined_df_top_10 = combined_df_top_10.drop(['Asset'],axis=1)

	#			combined_df_top_10.columns = ['Return (Over Period)','BTC Corr','ETH Corr','BTC Beta','ETH Beta','1d %','2d %','7d %','30d %','90d %']
	combined_df_top_10.columns = ['Return','BTC Corr','ETH Corr','BTC Beta','ETH Beta','1d %','3d %','7d %','30d %','90d %']




	##Similarly for bottom 10


	one_day_returns=[]
	three_day_returns=[]
	seven_day_returns=[]
	thirty_day_returns=[]
	ninety_day_returns=[]


	for i in final_bottom_10_symbols:
		price_data = binance_client.get_historical_klines(f'{i}','1h',f'{datetime.datetime.now()-timedelta(days=120)}')
		one_day_returns.append(((float(price_data[-1][4])/float(price_data[-24][4]))-1)*100)
		try:
			three_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-73][4]))-1)*100,2))
		except:
			three_day_returns.append(np.nan)
		try:
			seven_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-169][4]))-1)*100,2))
		except:
			seven_day_returns.append(np.nan)
		try:
			thirty_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-721][4]))-1)*100,2))
		except:
			thirty_day_returns.append(np.nan)
		try:
			ninety_day_returns.append(round(((float(price_data[-1][4])/float(price_data[-2161][4]))-1)*100,2))
		except:
			ninety_day_returns.append(np.nan)

	one_day_returns = pd.DataFrame(one_day_returns)
	one_day_returns.index = final_bottom_10_symbols
	one_day_returns['Asset'] = one_day_returns.index
	one_day_returns.columns = ['1D','Asset']


	three_day_returns = pd.DataFrame(three_day_returns)
	three_day_returns.index = final_bottom_10_symbols
	three_day_returns['Asset'] = three_day_returns.index
	three_day_returns.columns = ['3D','Asset']


	seven_day_returns = pd.DataFrame(seven_day_returns)
	seven_day_returns.index = final_bottom_10_symbols
	seven_day_returns['Asset'] = seven_day_returns.index
	seven_day_returns.columns = ['7D','Asset']


	thirty_day_returns = pd.DataFrame(thirty_day_returns)
	thirty_day_returns.index = final_bottom_10_symbols
	thirty_day_returns['Asset'] = thirty_day_returns.index
	thirty_day_returns.columns =['30D','Asset']

	ninety_day_returns = pd.DataFrame(ninety_day_returns)
	ninety_day_returns.index = final_bottom_10_symbols
	ninety_day_returns['Asset'] = ninety_day_returns.index
	ninety_day_returns.columns = ['90D','Asset']




	combined_df_bottom_10['Asset'] = combined_df_bottom_10.index
	combined_df_bottom_10 = pd.merge(combined_df_bottom_10,one_day_returns,on='Asset')
	combined_df_bottom_10 = pd.merge(combined_df_bottom_10,three_day_returns,on='Asset')
	combined_df_bottom_10 = pd.merge(combined_df_bottom_10,seven_day_returns,on='Asset')
	combined_df_bottom_10 = pd.merge(combined_df_bottom_10,thirty_day_returns,on='Asset')
	combined_df_bottom_10 = pd.merge(combined_df_bottom_10,ninety_day_returns,on='Asset')

	combined_df_bottom_10.index = combined_df_bottom_10['Asset']
	combined_df_bottom_10 = combined_df_bottom_10.drop(['Asset'],axis=1)

	#			combined_df_bottom_10.columns = ['Return (Over Period)','BTC Corr','ETH Corr','BTC Beta','ETH Beta','1d %','2d %','7d %','30d %','90d %']
	combined_df_bottom_10.columns = ['Return','BTC Corr','ETH Corr','BTC Beta','ETH Beta','1d %','3d %','7d %','30d %','90d %']

	combined_df_top_10 = combined_df_top_10.round(2)
	combined_df_bottom_10 = combined_df_bottom_10.round(2)

	print('*'*100)


	print('Top 10 Winners')
	print(combined_df_top_10)
	print('Top 10 Losers')
	print(combined_df_bottom_10)

	return btc_eth_cumret_df,combined_df_top_10,combined_df_bottom_10


