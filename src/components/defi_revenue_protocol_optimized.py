import pandas as pd

import requests
import numpy as np


def defi_protocol_revenue_bot(protocol_name):





	url = f'https://api.llama.fi/summary/fees/{protocol_name.lower()}?dataType=dailyRevenue'

	response = requests.get(url)
	response = response.json()



	#print(len(response['totalDataChart']))

	revenue_data = []
	daily_revenue_data = {}

	if response['totalDataChart'] is not None:
		for i in range(len(response['totalDataChart'])):
			revenue_data.append(response['totalDataChart'][i][1])

		###change in revenue append
		try:
			daily_revenue_data['1D Change'] = ((revenue_data[-1]/revenue_data[-2])-1)*100
		except:
			daily_revenue_data['1D Change'] = np.nan
		try:
			daily_revenue_data['3D Change'] = ((revenue_data[-1]/revenue_data[-4])-1)*100
		except:
			daily_revenue_data['3D Change'] = np.nan
		try:
			daily_revenue_data['7D Change'] = ((revenue_data[-1]/revenue_data[-8])-1)*100
		except:
			daily_revenue_data['7D Change'] = np.nan
		try:
			daily_revenue_data['14D Change'] = ((revenue_data[-1]/revenue_data[-15])-1)*100
		except:
			daily_revenue_data['14D Change'] = np.nan
		try:
			daily_revenue_data['30D Change'] = ((revenue_data[-1]/revenue_data[-31])-1)*100
		except:
			daily_revenue_data['30D Change'] = np.nan
		try:
			daily_revenue_data['90D Change'] = ((revenue_data[-1]/revenue_data[-91])-1)*100
		except:
			daily_revenue_data['90D Change'] = np.nan


		daily_revenue_data = pd.DataFrame.from_dict(daily_revenue_data,orient='index')
		daily_revenue_data.columns = [f'{protocol_name}']
		daily_revenue_data = daily_revenue_data.round(2)
		daily_revenue_data = daily_revenue_data.T

		print(daily_revenue_data)

		return daily_revenue_data

	else:
		daily_revenue_data = None
		return daily_revenue_data


