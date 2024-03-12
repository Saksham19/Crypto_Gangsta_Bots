import pandas as pd
import datetime
import discord
import requests


def defi_fees_protocols_bot(protocol_name):

		url = f'https://api.llama.fi/summary/fees/{protocol_name.lower()}?dataType=dailyFees'

		response = requests.get(url)
		response = response.json()

		print(response)

		# print(len(response['totalDataChart']))

		fees_data = []
		daily_fees_data = {}
		if response['totalDataChart'] is not None:

			for i in range(len(response['totalDataChart'])):
				fees_data.append(response['totalDataChart'][i][1])

			###change in fees append
			try:
				daily_fees_data['1D Change'] = ((fees_data[-1] / fees_data[-2]) - 1) * 100
			except:
				daily_fees_data['1D Change'] = np.nan
			try:
				daily_fees_data['3D Change'] = ((fees_data[-1] / fees_data[-4]) - 1) * 100
			except:
				daily_fees_data['3D Change'] = np.nan
			try:
				daily_fees_data['7D Change'] = ((fees_data[-1] / fees_data[-8]) - 1) * 100
			except:
				daily_fees_data['7D Change'] = np.nan
			try:
				daily_fees_data['14D Change'] = ((fees_data[-1] / fees_data[-15]) - 1) * 100
			except:
				daily_fees_data['14D Change'] = np.nan
			try:
				daily_fees_data['30D Change'] = ((fees_data[-1] / fees_data[-31]) - 1) * 100
			except:
				daily_fees_data['30D Change'] = np.nan
			try:
				daily_fees_data['90D Change'] = ((fees_data[-1] / fees_data[-91]) - 1) * 100
			except:
				daily_fees_data['90D Change'] = np.nan

			daily_fees_data = pd.DataFrame.from_dict(daily_fees_data, orient='index')
			daily_fees_data.columns = [f'{protocol_name.capitalize()}']
			daily_fees_data = daily_fees_data.round(2)
			daily_fees_data = daily_fees_data.T

			return daily_fees_data

		else:
			daily_fees_data = None
			return daily_fees_data
