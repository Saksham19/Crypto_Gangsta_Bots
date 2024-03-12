import pandas as pd
from tqdm import tqdm
import datetime
import discord
import requests
import threading
import time
import numpy as np


#print(info)



def stables_bot(sort_col):

		res = requests.get('https://stablecoins.llama.fi/stablecoincharts/all')
		res = res.json()

		date_data = []

		total_circ_data = []

		for i in range(len(res)):
			date_data.append(pd.to_datetime(datetime.datetime.fromtimestamp(int(res[i]['date'])),dayfirst=True))
			total_circ_data.append(res[i]['totalCirculatingUSD']['peggedUSD'])


		current_mcap = total_circ_data[-1]
		current_mcap = pd.DataFrame([current_mcap])

		one_day_gain_all = ((total_circ_data[-1]/total_circ_data[-2])-1)*100
		one_day_gain_all = pd.DataFrame([one_day_gain_all])

		three_day_gain_all = ((total_circ_data[-1]/total_circ_data[-4])-1)*100
		three_day_gain_all = pd.DataFrame([three_day_gain_all])

		seven_day_gain_all = ((total_circ_data[-1]/total_circ_data[-8])-1)*100
		seven_day_gain_all = pd.DataFrame([seven_day_gain_all])

		fourteen_day_gain_all = ((total_circ_data[-1]/total_circ_data[-15])-1)*100
		fourteen_day_gain_all = pd.DataFrame([fourteen_day_gain_all])

		twenty_day_gain_all = ((total_circ_data[-1]/total_circ_data[-21])-1)*100
		twenty_day_gain_all = pd.DataFrame([twenty_day_gain_all])

		thirty_day_gain_all = ((total_circ_data[-1]/total_circ_data[-31])-1)*100
		thirty_day_gain_all = pd.DataFrame([thirty_day_gain_all])

		sixty_day_gain_all = ((total_circ_data[-1]/total_circ_data[-61])-1)*100
		sixty_day_gain_all = pd.DataFrame([sixty_day_gain_all])

		ninety_day_gain_all = ((total_circ_data[-1]/total_circ_data[-91])-1)*100
		ninety_day_gain_all = pd.DataFrame([ninety_day_gain_all])

		four_month_day_gain_all = ((total_circ_data[-1]/total_circ_data[-121])-1)*100
		four_month_day_gain_all = pd.DataFrame([four_month_day_gain_all])

		stables_all_data = pd.concat([current_mcap/1000000000,one_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,three_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,seven_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,twenty_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,thirty_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,sixty_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,ninety_day_gain_all],axis=1)
		stables_all_data = pd.concat([stables_all_data,four_month_day_gain_all],axis=1)

		stables_all_data.columns = ['Current Mcap (bil)','1D Gain (%)','3D Gain (%)','7D Gain (%)','20D Gain (%)','30D Gain (%)','60D Gain (%)','90D Gain (%)','120D Gain (%)']
		stables_all_data['Current Mcap (bil)'] = stables_all_data['Current Mcap (bil)'].apply(lambda x: '{:.2f}'.format(x))
		stables_all_data = stables_all_data.round(2)
		stables_all_data.index = ['Totals']



		#BY CHAIN


		####Can just grab chain names and run a loop for this






		res = requests.get('https://api.llama.fi/chains')
		res = res.json()


		##grab the names nibba


		name_chain = []
		for i in range(len(res)):
			name_chain.append(res[i]['name'])





		def get_chain_stable_data(chain_name):


			res = requests.get(f'https://stablecoins.llama.fi/stablecoincharts/{chain_name}')
			res = res.json()
			df = []

			try:
				for j in range(len(res)):
					df.append(res[j]['totalCirculating']['peggedUSD'])
			except:
				pass


			return df



		klines = {}

		# Create a list to store the thread objects
		threads = []

		delay_between_requests = 0.00  # can adjust this value based on your rate limits

		# Start threads for each symbol
		for symbol in tqdm(name_chain):
			thread = threading.Thread(target=lambda s=symbol: klines.update({s: get_chain_stable_data(s)}))
			threads.append(thread)
			thread.start()

			time.sleep(delay_between_requests)

		# Wait for all threads to finish
		for thread in threads:
			thread.join()


		chain_stable_data = pd.DataFrame()

		chain_name = []
		for key,values in klines.items():
			chain_name.append(key)
			eth_circ = klines[key]
			try:
				eth_total_circ  = eth_circ[-1]/1000000
			except:
				eth_total_circ = np.nan

			try:
				eth_1d_gain = ((eth_circ[-1]/eth_circ[-2])-1)*100
			except:
				eth_1d_gain = np.nan

			try:
				eth_1w_gain = ((eth_circ[-1]/eth_circ[-7])-1)*100
			except:
				eth_1w_gain = np.nan

			try:
				eth_1m_gain = ((eth_circ[-1]/eth_circ[-30])-1)*100
			except:
				eth_1m_gain = np.nan

			eth_stable_data = [eth_total_circ]+[eth_1d_gain]+[eth_1w_gain]+[eth_1m_gain]
			eth_stable_data = pd.DataFrame(eth_stable_data).T
			eth_stable_data['Chain'] = [f'{i}']
			eth_stable_data.columns = ['Total','1D %','1W %','1M %','Chain']

			chain_stable_data = pd.concat([chain_stable_data,eth_stable_data])








		all_chain_stable = chain_stable_data.copy()

		all_chain_stable.index = chain_name
		all_chain_stable = all_chain_stable.drop(['Chain'],axis=1)



		all_chain_stable = all_chain_stable.dropna(subset=[f'{sort_col[0]}'])
		all_chain_stable =all_chain_stable.sort_values(f'{sort_col[0]}',ascending=False)
		all_chain_stable['Total'] = all_chain_stable['Total'].apply(lambda x: '{:.2f}'.format(x))
		all_chain_stable = all_chain_stable.rename(columns={'Total':'Total ($ mil)'})
		all_chain_stable = all_chain_stable.round(2)

		return stables_all_data,all_chain_stable

