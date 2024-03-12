from binance.client import Client as binance_client
import datetime
from datetime import timedelta
import discord
from src.components.markets_returns_bot_optimized import market_returns_bot
from src.components.volatility_bot_optimized import market_vol_bot
from src.components.stables_bot_optimized import stables_bot
from src.components.derivatives_bot_optimized import funding_bot
from src.components.defi_stats_optimized import defi_stats_bot
from src.components.defi_fees_optimized import defi_fees_bot
from src.components.defi_fees_categories_optimized import defi_fees_categories_bot
from src.components.defi_fees_protocols_optimized import defi_fees_protocols_bot
from src.components.defi_revenue_optimized import defi_revenue_bot
from src.components.defi_revenue_category_optimized import  defi_revenue_categories_bot
from src.components.defi_revenue_protocol_optimized import defi_protocol_revenue_bot
from src.components.defi_farming_optimized import farming_bot
from src.components.sector_high_level_breakdown import sector_bot
from src.components.specific_sector_breakdown import specific_sector_bot
from src.components.address_wallet_tracker import wallet_bot
from src.components.agg_wallet_tracker import agg_wallet_bot
from src.components.specific_token_portfolio import token_whales_bot
from dotenv import load_dotenv
import os

load_dotenv()
binance_client = binance_client()
info = binance_client.get_exchange_info()

######DISCORD STUFFF

# load_dotenv()


TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
binance_secret = os.getenv('BINANCE_FUTURES_SECRET_KEY')
binance_api = os.getenv('BINANCE_FUTURES_API_KEY')


class MyClient(discord.Client):
    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')

    async def on_message(self, message: object):
        # we do not want the bot to reply to itself
        if message.author.id == self.user.id:
            return

        ####market_update#######


        if message.content.startswith('MARKET_STATS'):
            if message.content == 'MARKET_STATS_1D':
                duration = '24 hours ago UTC'
                days_t = 1
            elif message.content == 'MARKET_STATS_2D':
                duration = '48 hours ago UTC'
                days_t = 2
            elif message.content == 'MARKET_STATS_7D':
                duration = '168 hours ago UTC'
                days_t = 7
            elif message.content == 'MARKET_STATS_14D':
                duration = '336 hours ago UTC'
                days_t = 14
            elif message.content == 'MARKET_STATS_30D':
                duration = '720 hours ago UTC'
                days_t = 30

            btc_eth_cumret_df, combined_df_top_10, combined_df_bottom_10 = market_returns_bot(duration)

            ################DISCORD WORK############################

            ####
            #            embed = discord.Embed(title='Market Stats')
            #            embed.add_field(name='Top 10 perf',value=(combined_df_bottom_10.to_markdown(tablefmt='grid')),inline=True)
            #            await message.reply(f'MARKET STATS between {(datetime.now() - timedelta(days=2)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(
                f'BTC/ETH Returns between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + btc_eth_cumret_df.to_markdown(index=True) + '```')
            await message.reply(
                f'Top 10 Performers between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + combined_df_top_10.to_markdown(index=True) + '```')
            await message.reply(
                f'Bottom 10 Performers between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + combined_df_bottom_10.to_markdown(index=True) + '```')


        #####VOL BOT

        elif message.content.startswith('VOL_STATS'):

            if message.content == 'VOL_STATS_3D':
                duration = '72 hours ago UTC'
                days_t = 3

            elif message.content == 'VOL_STATS_7D':

                duration = '168 hours ago UTC'
                days_t = 7

            elif message.content == 'VOL_STATS_14D':

                duration = '336 hours ago UTC'
                days_t = 14

            elif message.content == 'VOL_STATS_30D':

                duration = '720 hours ago UTC'
                days_t = 30

            btc_eth_vol_df, combined_df_top_10, combined_df_bottom_10 = market_vol_bot(duration)

            await message.reply(f'BTC/ETH Vol between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + btc_eth_vol_df.to_markdown(index=True) + '```')
            await message.reply(f'Top 10 Vol between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + combined_df_top_10.to_markdown(index=True) + '```')
            await message.reply(f'Bottom 10 Vol between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + combined_df_bottom_10.to_markdown(index=True) + '```')


        #####STABLE BOT ######

        elif message.content.startswith('STABLE_STATS'):
            if message.content == 'STABLE_STATS_TOTAL':
                sort_col = ['Total']
            elif message.content == 'STABLE_STATS_1D':
                sort_col = ['1D %']
            elif message.content == 'STABLE_STATS_7D':
                sort_col = ['1W %']
            elif message.content == 'STABLE_STATS_30D':
                sort_col = ['1M %']

            stables_all_data, all_chain_stable = stables_bot(sort_col)

            await message.reply(f'Stablecoin Data as of {datetime.datetime.now()}')
            await message.reply(f'```' + stables_all_data.T.to_markdown(index=True) + '```')
            await message.reply(f'```' + all_chain_stable.iloc[:15, :].to_markdown(index=True) + '```')
            await message.reply(f'```' + all_chain_stable.iloc[15:30, :].to_markdown(index=True) + '```')


            ####fUNDING STATS

        elif message.content.startswith('FUNDING_STATS'):

            if message.content == 'FUNDING_STATS_1D':
                days_t = 1
            elif message.content == 'FUNDING_STATS_2D':
                days_t = 2
            elif message.content == 'FUNDING_STATS_7D':
                days_t = 7
            elif message.content == 'FUNDING_STATS_14D':
                days_t = 14
            elif message.content == 'FUNDING_STATS_30D':
                days_t = 30

            btc_eth_funding_stats, top_10_stats, bottom_10_stats = funding_bot(days_t,binance_secret,binance_api)


            await message.reply(
                f'Funding data between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'```' + btc_eth_funding_stats.to_markdown(index=True) + '```')
            await message.reply(f'Top 15 Sorted by Funding')
            await message.reply(f'```' + top_10_stats.to_markdown(index=True) + '```')
            await message.reply(f'Bottom 15 Sorted by Funding')
            await message.reply(f'```' + bottom_10_stats.to_markdown(index=True) + '```')


        #####DEFI STATS #######


        elif message.content.startswith('DEFI_STATS'):
            if message.content == 'DEFI_STATS_1D':
                days_t = 1
            elif message.content == 'DEFI_STATS_3D':
                days_t = 3
            elif message.content == 'DEFI_STATS_7D':
                days_t = 7
            elif message.content == 'DEFI_STATS_14D':
                days_t = 14
            elif message.content == 'DEFI_STATS_30D':
                days_t = 30
            elif message.content == 'DEFI_STATS_90D':
                days_t = 90

            tvl_change_df_top_10, tvl_change_df_top_20, tvl_change_df_top_30, tvl_change_df_bottom_10, vol_change_df_top_10, vol_change_df_top_20, vol_change_df_top_30, vol_change_df_bottom_10 = defi_stats_bot(days_t)

            await message.reply(
                f'DeFi Chain TVL Stats Between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'DeFi Top 30 Chains By TVL - {days_t}D %')
            await message.reply(f'```' + tvl_change_df_top_10.to_markdown(index=True) + '```')
            await message.reply(f'```' + tvl_change_df_top_20.to_markdown(index=True) + '```')
            await message.reply(f'```' + tvl_change_df_top_30.to_markdown(index=True) + '```')
            await message.reply(f'DeFi Bottom 10 Chains By TVL - {days_t}D %')
            await message.reply(f'```' + tvl_change_df_bottom_10.to_markdown(index=True) + '```')

            ##vol stuff

            await message.reply(
                f'DeFi Chain Volumes Stats Between {(datetime.datetime.now() - timedelta(days=days_t)).strftime("%d/%m/%Y %H:%M:%S")} - {datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            await message.reply(f'DeFi Top 30 Chains By Volume - {days_t}D %')
            await message.reply(f'```' + vol_change_df_top_10.to_markdown(index=True) + '```')
            await message.reply(f'```' + vol_change_df_top_20.to_markdown(index=True) + '```')
            await message.reply(f'```' + vol_change_df_top_30.to_markdown(index=True) + '```')
            await message.reply(f'DeFi Bottom 10 Chains By Volume - {days_t}D %')
            await message.reply(f'```' + vol_change_df_bottom_10.to_markdown(index=True) + '```')

        ####DEFI CHAIN STATS - NEED TO FIX



        ###DEFI FEES STATS
        elif message.content.startswith('DEFI_FEES_STATS'):

            if '1D' in message.content:
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

            agg_df_parts, protocol_fees_data_df_top_10 = defi_fees_bot(days_t)

            await message.reply(f'DeFi Fees By Category as of {datetime.datetime.now()}')

            # Send each part in separate messages
            for i, part in enumerate(agg_df_parts):
                await message.reply(f'DeFi Fees By Category (Part {i + 1}) as of {datetime.datetime.now()}')
                await message.reply(f'```' + part.to_markdown(index=True) + '```')

            await message.reply(
                f'DeFi Top 30 Protocols by Fees Sorted by {days_t}D Change as of {datetime.datetime.now()}')
            await message.reply(f'```' + protocol_fees_data_df_top_10.iloc[:10].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_fees_data_df_top_10.iloc[10:20].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_fees_data_df_top_10.iloc[20:30].to_markdown(index=True) + '```')


        ####DEFI FEES CATEGORIES

        elif message.content.startswith('DEFI_CATEGORY_FEES_STATS'):

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

            message.content = message.content.replace("DEFI_CATEGORY_FEES_STATS_", "")
            message_content = message.content.replace(f"_{days_t}D", "")

            protocol_fees_data_df_category = defi_fees_categories_bot(days_t,message_content)


            await message.reply(
                f'DeFi Top 30 Protocols by Fees for {message.content} Sorted by {days_t}D Change as of {datetime.datetime.now()}')
            await message.reply(f'```' + protocol_fees_data_df_category.iloc[:10].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_fees_data_df_category.iloc[10:20].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_fees_data_df_category.iloc[20:30].to_markdown(index=True) + '```')


         ######DEFI PROTOCOL FEES STATS

        elif message.content.startswith('DEFI_PROTOCOL_FEES_STATS'):

            protocol_name = message.content.replace("DEFI_PROTOCOL_FEES_STATS_", "")

            daily_fees_data = defi_fees_protocols_bot(protocol_name)

            if daily_fees_data is not None:
                await message.reply(f'DeFi {protocol_name} Fees Stats as of {datetime.datetime.now()}')
                await message.reply(f'```' + daily_fees_data.to_markdown(index=True) + '```')

            else:
                await message.reply(f'No fees stats for {protocol_name}')


        ####DEFI REVENUE STATS#######

        elif message.content.startswith('DEFI_REVENUE_STATS'):

            if '1D' in message.content:
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

            agg_df_parts, protocol_revenue_data_df_top_10 = defi_revenue_bot(days_t)


            await message.reply(f'DeFi Revenue By Category as of {datetime.datetime.now()}')


            # Send each part in separate messages
            for i, part in enumerate(agg_df_parts):
                await message.reply(f'DeFi Revenue By Category (Part {i + 1}) as of {datetime.datetime.now()}')
                await message.reply(f'```' + part.to_markdown(index=True) + '```')

            await message.reply(f'DeFi Top 30 Protocols by Revenue Sorted by {days_t}D Change as of {datetime.datetime.now()}')
            await message.reply(f'```' + protocol_revenue_data_df_top_10.iloc[:10].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_revenue_data_df_top_10.iloc[10:20].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_revenue_data_df_top_10.iloc[20:30].to_markdown(index=True) + '```')

        ######DEFI REVENUE CATEGORY#######

        elif message.content.startswith('DEFI_CATEGORY_REVENUE_STATS'):
            if '1D' in message.content:
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

            message.content = message.content.replace("DEFI_CATEGORY_REVENUE_STATS_", "")
            message_content = message.content.replace(f"_{days_t}D", "")

            protocol_revenue_data_df_category = defi_revenue_categories_bot(days_t,message_content)

            await message.reply(
                f'DeFi Top 30 Protocols by Revenue for {message.content} Sorted by {days_t}D Change as of {datetime.datetime.now()}')
            await message.reply(f'```' + protocol_revenue_data_df_category.iloc[:10].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_revenue_data_df_category.iloc[10:20].to_markdown(index=True) + '```')
            await message.reply(f'```' + protocol_revenue_data_df_category.iloc[20:30].to_markdown(index=True) + '```')


        #####defi protocol revenue
        elif message.content.startswith('DEFI_PROTOCOL_REVENUE_STATS'):
            protocol_name = message.content.replace("DEFI_PROTOCOL_REVENUE_STATS_", "")

            daily_revenue_data = defi_protocol_revenue_bot(protocol_name)

            if daily_revenue_data is not None:
                await message.reply(f'DeFi {protocol_name.capitalize()} Revenue Stats as of {datetime.datetime.now()}')
                await message.reply(f'```' + daily_revenue_data.to_markdown(index=True) + '```')


            else:
                await message.reply(f'No revenue data available for {protocol_name.capitalize()}')



        ####FARMING##########

        elif message.content.startswith('ALL_POOL_STATS'):

            message_content = message.content

            combined_pools_10 = farming_bot(message_content)

            if 'TVL' in message_content:
                await message.reply(f'DeFi Top 10 Pools by TVL as of {datetime.datetime.now()}')
                await message.reply(f'```' + combined_pools_10.to_markdown(index=True) + '```')



            elif 'APY' in message.content:

                await message.reply(f'DeFi Top 10 Pools by APY as of {datetime.datetime.now()}')
                await message.reply(f'```' + combined_pools_10.to_markdown(index=True) + '```')


        elif message.content.startswith('STABLE_POOL_STATS'):

            message_content = message.content
            combined_pools_10 = farming_bot(message_content)

            if 'TVL' in message.content:

                await message.reply(f'DeFi Top 10 Stable Pools by TVL as of {datetime.datetime.now()}')
                await message.reply(f'```' + combined_pools_10.to_markdown(index=True) + '```')

            elif 'APY' in message.content:

                await message.reply(f'DeFi Top 10 Stable Pools by APY as of {datetime.datetime.now()}')
                await message.reply(f'```' + combined_pools_10.to_markdown(index=True) + '```')



        #####SECTOR ANALYSIS

        elif message.content.startswith('ALL_SECTOR_STATS'):

            message_content = message.content

            if 'PRICE' in message.content:
                cat_10, cat_20, combined_sector_df = sector_bot(message_content)


                await message.reply(f'Top 20 sectors sorted by average price change as of {datetime.datetime.now()}')
                await message.reply(f'```' + cat_10.to_markdown(index=False) + '```')
                await message.reply(f'```' + cat_20.to_markdown(index=False) + '```')

                await message.reply(f'Vol % Share')
                await message.reply(f'```' + combined_sector_df.to_markdown() + '```')
    #            await message.reply(f'```' + cat_20.to_markdown(index=False) + '```')


            elif 'MCAP' in message.content:

                cat_10, cat_20, combined_sector_df = sector_bot(message_content)

                await message.reply(f'Vol % Share')
                await message.reply(f'```' + combined_sector_df.to_markdown() + '```')

            elif 'VOL' in message.content:

                cat_10,cat_20,combined_sector_df = sector_bot(message_content)


                await message.reply(f'Top 20 sectors sorted by average vol change as of {datetime.datetime.now()}')
                await message.reply(f'```' + cat_10.to_markdown(index=False) + '```')
                await message.reply(f'```' + cat_20.to_markdown(index=False) + '```')

                await message.reply(f'Vol % Share')
                await message.reply(f'```' + combined_sector_df.to_markdown() + '```')


        ####specific sector

        elif message.content.startswith('SPECIFIC_SECTOR_STATS_'):

            message_content = message.content.replace("SPECIFIC_SECTOR_STATS_", "")

            combined_sector_df,combined_address_data_df = specific_sector_bot(message_content)


            await message.reply(f'{message.content} Aggregated Volumes Data')
            await message.reply(f'```' + combined_sector_df.to_markdown() + '```')

            await message.reply(f'{message.content} Wallet Holders Breakdown')
            await message.reply(f'```' + combined_address_data_df.to_markdown() + '```')



    ############ WALLET ADDRESS TRACKER ######

        elif message.content.startswith('WALLET_STATS_'):
            message.content = message.content.replace("WALLET_STATS_", "")
            message_content = message.content

            final_one_day_port,hist_port_values_chain = wallet_bot(message_content)

            await message.reply(f'{message.content} Portfolio Holdings')
            await message.reply(f'```' + final_one_day_port.to_markdown() + '```')
            await message.reply(f'{message.content} Portfolio Holdings (Chain)')
            await message.reply(f'```' + hist_port_values_chain.to_markdown() + '```')


        ############ AGG WALLET FUNDS ###########


        elif message.content.startswith('AGG_WALLET'):


        ####load addresses from wallet_list excel file

            combined_hist_port_1d_all = agg_wallet_bot()

            await message.reply('Fund Portfolio')
            await message.reply(f'```' + combined_hist_port_1d_all.to_markdown() + '```')


    ########### TOKEN WALLET TRACKER #########

        elif message.content.startswith('TOKEN_WHALES_'):

            message.content = message.content.replace("TOKEN_WHALES_", "")
            message_content = message.content

            combined_address_data_df = token_whales_bot(message_content)

            await message.reply(f'{message.content} Whale Portfolio')
            await message.reply(f'```' + combined_address_data_df.to_markdown() + '```')


intents = discord.Intents.default()
intents.message_content = True

client = MyClient(intents=intents)
client.run(TOKEN)
