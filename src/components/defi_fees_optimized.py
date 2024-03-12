import pandas as pd
import numpy as np
import requests


def defi_fees_bot(days_t):

		fees_data = requests.get('https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees')
		fees_data = fees_data.json()




		print(fees_data)


		protocol_fees_data = {}
		for i in range(len(fees_data['protocols'])):
			protocol_fees_data[fees_data['protocols'][i]['name']] = [fees_data['protocols'][i]['category'],fees_data['protocols'][i]['change_1d'],fees_data['protocols'][i]['change_7d'],fees_data['protocols'][i]['change_1m']]




		####we can do aggregate fees by category and show trends?


		protocol_fees_data_df  = pd.DataFrame.from_dict(protocol_fees_data,orient='index')
		protocol_fees_data_df.columns = ['Category','Change 1D','Change 7D','Change 30D']


		##aggregate data

		agg_df = protocol_fees_data_df.groupby('Category').agg({
			'Change 1D': 'median','Change 7D':'median','Change 30D':'median'})


		agg_df.columns = ['Median Change 1D','Median Change 7D','Median Change 30D']
		agg_df = agg_df.round(2)

		agg_df  = agg_df.sort_values(f'Median Change {days_t}D',ascending=False)


		print(agg_df)




		###rank these by change in fees over certain days

		protocol_fees_data_df_top_10 = protocol_fees_data_df.sort_values(f'Change {days_t}D',ascending=False)  ###can change this based on what timeframe we looking at
		protocol_fees_data_df_top_10 = protocol_fees_data_df_top_10.dropna()


		print(protocol_fees_data_df_top_10)


		agg_df_parts = np.array_split(agg_df, 3)

		return agg_df_parts,protocol_fees_data_df_top_10
