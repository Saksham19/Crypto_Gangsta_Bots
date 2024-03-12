import pandas as pd
import datetime
import discord
import requests


def defi_fees_categories_bot(days_t,message_content):


	fees_data = requests.get('https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees')
	fees_data = fees_data.json()


	protocol_fees_data = {}
	for i in range(len(fees_data['protocols'])):
		protocol_fees_data[fees_data['protocols'][i]['name']] = [fees_data['protocols'][i]['category'],fees_data['protocols'][i]['change_1d'],fees_data['protocols'][i]['change_7d'],fees_data['protocols'][i]['change_1m']]



	####we can do aggregate fees by category and show trends?


	protocol_fees_data_df  = pd.DataFrame.from_dict(protocol_fees_data,orient='index')
	protocol_fees_data_df.columns = ['Category','Change 1D','Change 7D','Change 30D']




	print(protocol_fees_data_df['Category'].values)
	####we can filter by specific category to see how protocols/items look under each one

	message_content = message_content.replace('_', ' ')
	protocol_fees_data_df_category =  protocol_fees_data_df[protocol_fees_data_df['Category']==f'{message_content.title()}']
	protocol_fees_data_df_category = protocol_fees_data_df_category.sort_values('Change 7D',ascending=False)
	protocol_fees_data_df_category  = protocol_fees_data_df_category.dropna()

	return protocol_fees_data_df_category


