import pandas as pd
import datetime
import discord
import requests
import numpy as np

def defi_revenue_bot(days_t):


		####we should probably do chain level first?
		###or do we do general  - then breakdown by chain level? - probably this

		revenue_data = requests.get('https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue')
		revenue_data = revenue_data.json()




		print(revenue_data)


		protocol_revenue_data = {}
		for i in range(len(revenue_data['protocols'])):
			protocol_revenue_data[revenue_data['protocols'][i]['name']] = [revenue_data['protocols'][i]['category'],revenue_data['protocols'][i]['change_1d'],revenue_data['protocols'][i]['change_7d'],revenue_data['protocols'][i]['change_1m']]




		####we can do aggregate revenue by category and show trends?


		protocol_revenue_data_df  = pd.DataFrame.from_dict(protocol_revenue_data,orient='index')
		protocol_revenue_data_df.columns = ['Category','Change 1D','Change 7D','Change 30D']


		##aggregate data

		agg_df = protocol_revenue_data_df.groupby('Category').agg({
			'Change 1D': 'median','Change 7D':'median','Change 30D':'median'})


		agg_df.columns = ['Median Change 1D','Median Change 7D','Median Change 30D']
		agg_df = agg_df.round(2)



		print(agg_df)
		agg_df  = agg_df.sort_values(f'Median Change {days_t}D',ascending=False)



		###rank these by change in revenue over certain days

		protocol_revenue_data_df_top_10 = protocol_revenue_data_df.sort_values(f'Change {days_t}D',ascending=False)  ###can change this based on what timeframe we looking at
		protocol_revenue_data_df_top_10 = protocol_revenue_data_df_top_10.dropna()


		print(protocol_revenue_data_df_top_10)



		agg_df_parts = np.array_split(agg_df, 3)

		return agg_df_parts,protocol_revenue_data_df_top_10
