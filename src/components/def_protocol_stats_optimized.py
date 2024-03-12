import pandas as pd
from tqdm import tqdm
import datetime
from datetime import timedelta
import discord
import requests
import threading
import time
import numpy as np
from urllib.request import Request, urlopen
import re


#print(info)


######DISCORD STUFFF

# load_dotenv()
# TOKEN = os.getenv('DISCORD_TOKEN')
# GUILD = os.getenv('DISCORD_GUILD')

TOKEN = 'MTA3MTQzODI2Njk4Mjc0NDA3NA.GEWzSL.TNDkxPc6PZxX6hsiKZGlIxUzYk9v-tgSmNV-Rg'
GUILD = "Saksham1212's server"

class MyClient(discord.Client):
	async def on_ready(self):
		print(f'Logged in as {self.user} (ID: {self.user.id})')
		print('------')

	async def on_message(self, message):
		# we do not want the bot to reply to itself
		if message.author.id == self.user.id:
			return
		if message.content.startswith('DEFI_CHAIN_STATS'):

			if '1D'in message.content:
				days_t = 1
			elif '3D' in message.content:
				days_t = 3
			elif '7D' in message.content:
				days_t = 7
			elif '14D' in message.content:
				days_t = 14
			elif '30D' in message.content:
				days_t = 30
			elif '90D' in message.content:
				days_t = 90

		message.content = message.content.replace("DEFI_CHAIN_STATS_", "")
		message.content = message.content.replace(f"_{days_t}D", "")
		#			message.content = message.content.title()


		res = requests.get('https://api.llama.fi/chains')
		res = res.json()

		chain_names = []
		for i in range(len(res)):
			if res[i]['name'] is not None:
				chain_names.append(res[i]['name'])

		# name = message.content
		name = message.content
		print(name)

		chain_names = [x.replace(' ', '%20') for x in chain_names]

		chain_name = None

		for crypto in chain_names:
			if crypto.lower() == name.lower():
				chain_name = crypto
				break

		if chain_name is None:
			for crypto in chain_names:
				if name.lower() in crypto.lower():
					chain_name = crypto
					break



		message.content = chain_name


		url = f'https://defillama.com/chain/{chain_name}'

		req = Request(
			url=url,
			headers={'User-Agent': 'Mozilla/5.0'}
		)

		webpage = urlopen(req).read()

		# response = requests.get(url,headers={'User-Agent': 'Mozilla/5.0'})

		print(webpage)
		dfs = pd.read_html(webpage)

		dfs = dfs[1]

		new_col_names = []
		for i in range(len(dfs.columns)):
			new_col_names.append(dfs.columns[i][1])

		dfs.columns = new_col_names

		dfs = dfs.dropna(subset=['Name'])

		name_list = list(dfs['Name'])
		change_1d = list(dfs['1d Change'])
		change_7d = list(dfs['7d Change'])
		change_30d = list(dfs['1m Change'])

		name_list = [str(x) for x in name_list]

		print(name_list)

		clean_names = []

		# iterate over each string in the original list
		for s in name_list:
			# remove the word 'chains' from the end of the string
			s = s.replace(' chains', '')
			s = s.replace(' chain', '')

			clean_words = s[1:]
			clean_name = clean_words[:-1]

			# add the clean name to the new list
			clean_names.append(clean_name)

		print(clean_names)

		##Can use the api to get the protocol level TVL stats

		##First  - need to fix the names to api appropriate standards  - everything in small and space replaced by '-'

		clean_names_api = [x.lower() for x in clean_names]
		clean_names_api = [x.replace(' ', '-') for x in clean_names_api]

		print(clean_names_api)

		###api call for hist tvl here

		# clean_names_api = clean_names_api[:2]


		# for i in range(len(res)):


		change_1d = {}
		change_3d = {}
		change_7d = {}
		change_14d = {}
		change_30d = {}
		change_90d = {}


		def defi_protocol_data(protocol_name):
			df = []
			try:
				res = requests.get(f'https://api.llama.fi/protocol/{protocol_name}')
				res = res.json()

				for i in range(len(res['tvl'])):
					df.append(res['tvl'][i]['totalLiquidityUSD'])
			except:
				j = re.sub(r"\d+", "", protocol_name)
				res = requests.get(f'https://api.llama.fi/protocol/{j}')
				res = res.json()
				for i in range(len(res['tvl'])):
					df.append(res['tvl'][i]['totalLiquidityUSD'])

			return df


		klines = {}

		# Create a list to store the thread objects
		threads = []

		delay_between_requests = 0.00  # can adjust this value based on your rate limits

		# Start threads for each symbol
		for symbol in tqdm(clean_names_api):
			thread = threading.Thread(target=lambda s=symbol: klines.update({s: defi_protocol_data(s)}))
			threads.append(thread)
			thread.start()

			time.sleep(delay_between_requests)

		# Wait for all threads to finish
		for thread in threads:
			thread.join()

		for j, token_tvl in klines.items():

			try:
				change_1d[j] = ((token_tvl[-1] / token_tvl[-2]) - 1) * 100
			except:
				change_1d[j] = np.nan

			try:
				change_3d[j] = ((token_tvl[-1] / token_tvl[-4]) - 1) * 100
			except:
				change_3d[j] = np.nan

			try:
				change_7d[j] = ((token_tvl[-1] / token_tvl[-8]) - 1) * 100
			except:
				change_7d[j] = np.nan

			try:
				change_14d[j] = ((token_tvl[-1] / token_tvl[-15]) - 1) * 100
			except:
				change_14d[j] = np.nan

			try:
				change_30d[j] = ((token_tvl[-1] / token_tvl[-31]) - 1) * 100
			except:
				change_30d[j] = np.nan

			try:
				change_90d[j] = ((token_tvl[-1] / token_tvl[-91]) - 1) * 100
			except:
				change_90d[j] = np.nan

		change_1d = pd.DataFrame.from_dict(change_1d, orient='index')
		change_3d = pd.DataFrame.from_dict(change_3d, orient='index')
		change_7d = pd.DataFrame.from_dict(change_7d, orient='index')
		change_14d = pd.DataFrame.from_dict(change_14d, orient='index')
		change_30d = pd.DataFrame.from_dict(change_30d, orient='index')
		change_90d = pd.DataFrame.from_dict(change_90d, orient='index')

		tvl_change_df = pd.concat([change_1d, change_3d, change_7d, change_14d, change_30d, change_90d], axis=1)
		tvl_change_df.columns = ['1D %', '3D %', '7D %', '14D %', '30D %', '90D %']
		tvl_change_df = tvl_change_df.round(2)

		###we can basically rank the tvl's based on user input - if want 1d - then rank by 1d - if want 3d - rank by 3d etc.
		tvl_change_df = pd.concat([tvl_change_df], axis=1)
		tvl_change_df.columns = ['1D %', '3D %', '7D %', '14D %', '30D %', '90D %']

		###we can basically rank the tvl's based on user input - if want 1d - then rank by 1d - if want 3d - rank by 3d etc.
		tvl_change_df = tvl_change_df.dropna(subset=[f'{days_t}D %'])
		tvl_change_df_sorted = tvl_change_df.sort_values(f'{days_t}D %',
														 ascending=False)  ###can change this based on what timeframe we looking at

		##Grab the top 20 and bottom 20?
		tvl_change_df_top_10 = tvl_change_df_sorted.iloc[:10]
		tvl_change_df_top_20 = tvl_change_df_sorted.iloc[10:20]
		tvl_change_df_top_30 = tvl_change_df_sorted.iloc[20:30]
		tvl_change_df_bottom_10 = tvl_change_df_sorted.tail(10)

		tvl_change_df_bottom_10 = tvl_change_df_bottom_10.sort_values(f'{days_t}D %',
																	  ascending=False)  ###can change this based on what timeframe we looking at


		await message.reply(
			f'DeFi Chain Protocol TVL Stats Between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
		await message.reply(f'DeFi Top 30 Protocols By TVL on {message.content}- {days_t}D %')
		await message.reply(f'```' + tvl_change_df_top_10.to_markdown(index=True) + '```')
		await message.reply(f'```' + tvl_change_df_top_20.to_markdown(index=True) + '```')
		await message.reply(f'```' + tvl_change_df_top_30.to_markdown(index=True) + '```')
		await message.reply(f'DeFi Bottom 10 Protocols By TVL on {message.content}- {days_t}D %')
		await message.reply(f'```' + tvl_change_df_bottom_10.to_markdown(index=True) + '```')


intents = discord.Intents.default()
intents.message_content = True

client = MyClient(intents=intents)
client.run(TOKEN)


