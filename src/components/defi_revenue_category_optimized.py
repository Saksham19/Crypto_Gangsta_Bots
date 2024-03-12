import pandas as pd
import requests



def defi_revenue_categories_bot(days_t,message_content):


####we should probably do chain level first?
		###or do we do general  - then breakdown by chain level? - probably this

		revenue_data = requests.get('https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue')
		revenue_data = revenue_data.json()


		protocol_revenue_data = {}
		for i in range(len(revenue_data['protocols'])):
			protocol_revenue_data[revenue_data['protocols'][i]['name']] = [revenue_data['protocols'][i]['category'],revenue_data['protocols'][i]['change_1d'],revenue_data['protocols'][i]['change_7d'],revenue_data['protocols'][i]['change_1m']]



		####we can do aggregate revenue by category and show trends?


		protocol_revenue_data_df  = pd.DataFrame.from_dict(protocol_revenue_data,orient='index')
		protocol_revenue_data_df.columns = ['Category','Change 1D','Change 7D','Change 30D']




		####we can filter by specific category to see how protocols/items look under each one

		protocol_revenue_data_df_category =  protocol_revenue_data_df[protocol_revenue_data_df['Category']==f'{message_content.title()}']
		protocol_revenue_data_df_category = protocol_revenue_data_df_category.sort_values(f'Change {days_t}D',ascending=False)
		protocol_revenue_data_df_category  = protocol_revenue_data_df_category.dropna()

		return protocol_revenue_data_df_category

