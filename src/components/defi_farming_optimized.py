import pandas as pd
import datetime
import discord
import requests
import numpy as np



def farming_bot(message_content):

    if message_content.startswith('ALL_POOL_STATS'):

        pools_data = requests.get('https://yields.llama.fi/pools')
        pools_data = pools_data.json()

        tvl_value = []
        pool_name = []
        chain_name = []
        current_apy = []
        pool_symbol = []
        apy_1d = []
        apy_7d = []
        apy_30d = []
        stable = []
        project = []

        for i in range(len(pools_data['data'])):
            tvl_value.append(pools_data['data'][i]['tvlUsd'])
            pool_name.append(pools_data['data'][i]['pool'])
            chain_name.append(pools_data['data'][i]['chain'])
            pool_symbol.append(pools_data['data'][i]['symbol'])
            current_apy.append(pools_data['data'][i]['apy'])
            apy_1d.append(pools_data['data'][i]['apyPct1D'])
            apy_7d.append(pools_data['data'][i]['apyPct7D'])
            apy_30d.append(pools_data['data'][i]['apyPct30D'])
            stable.append(pools_data['data'][i]['stablecoin'])
            project.append(pools_data['data'][i]['project'])

        tvl_value = pd.DataFrame(tvl_value)
        pool_name = pd.DataFrame(pool_name)
        chain_name = pd.DataFrame(chain_name)
        pool_symbol = pd.DataFrame(pool_symbol)
        current_apy = pd.DataFrame(current_apy)
        apy_1d = pd.DataFrame(apy_1d)
        apy_7d = pd.DataFrame(apy_7d)
        apy_30d = pd.DataFrame(apy_30d)
        stable = pd.DataFrame(stable)
        project = pd.DataFrame(project)

        combined_pools = pd.concat(
            [chain_name, pool_name, tvl_value, pool_symbol, current_apy, apy_1d, apy_7d, apy_30d, project, stable],
            axis=1)

        combined_pools.columns = ['Chain', 'Pool', 'TVL', 'Symbol', 'APY', 'APY 1D', 'APY 7D', 'APY 30D', 'Project',
                                  'Stable']

        stable_all_pools = combined_pools[combined_pools['Stable'] == True]
        print(stable_all_pools)

        stable_all_pools = stable_all_pools.drop(['Stable'], axis=1)

        if 'TVL' in message_content:

            ##get the top 10 by tvl

            combined_pools_tvl_top_10 = combined_pools.sort_values('TVL',
                                                                   ascending=False)  ###can change this based on what timeframe we looking at
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.head(10)
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10[
                ['Pool', 'Symbol', 'Project', 'Chain', 'TVL', 'APY', 'APY 7D', 'APY 30D']]

            pool_names = list(combined_pools_tvl_top_10['Pool'])

            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['Pool'], axis=1)

            ####MAY WANNA ADD TVL/APY TREND OVER LAST 30 DAYS

            ###Get data for all pools and save as excel file

            tvl_7d_change = {}
            tvl_30d_change = {}

            apy_30d_mean = {}

            for i in pool_names:
                hist_data = requests.get(f'https://yields.llama.fi/chart/{i}').json()
                hist_tvl = []
                hist_apy = []
                for j in range(len(hist_data['data'])):
                    hist_tvl.append(hist_data['data'][j]['tvlUsd'])
                    hist_apy.append(hist_data['data'][j]['apy'])

                try:
                    tvl_7d_change[i] = (hist_tvl[-1] / hist_tvl[-8]) - 1
                except:
                    tvl_7d_change[i] = np.nan
                try:
                    tvl_30d_change[i] = (hist_tvl[-1] / hist_tvl[-31]) - 1
                except:
                    tvl_30d_change[i] = np.nan
                try:
                    apy_30d_mean[i] = (pd.Series(hist_apy).rolling(30).mean().iloc[-1])
                except:
                    apy_30d_mean[i] = np.nan

            tvl_7d_change_df = pd.DataFrame.from_dict(tvl_7d_change, orient='index')
            tvl_30d_change_df = pd.DataFrame.from_dict(tvl_30d_change, orient='index')
            apy_30d_mean_df = pd.DataFrame.from_dict(apy_30d_mean, orient='index')

            tvl_7d_change_df.columns = ['TVL 7D %']
            tvl_30d_change_df.columns = ['TVL 30D %']
            apy_30d_mean_df.columns = ['APY 30D M']

            tvl_7d_change_df = tvl_7d_change_df.reset_index()
            tvl_7d_change_df = tvl_7d_change_df.drop(['index'], axis=1)

            tvl_30d_change_df = tvl_30d_change_df.reset_index()
            tvl_30d_change_df = tvl_30d_change_df.drop(['index'], axis=1)

            apy_30d_mean_df = apy_30d_mean_df.reset_index()
            apy_30d_mean_df = apy_30d_mean_df.drop(['index'], axis=1)

            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.reset_index()
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['index'], axis=1)

            combined_pools_tvl_top_10 = pd.concat(
                [combined_pools_tvl_top_10, tvl_7d_change_df, tvl_30d_change_df, apy_30d_mean_df], axis=1)
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['APY 30D'], axis=1)
            combined_pools_tvl_top_10['TVL'] = combined_pools_tvl_top_10['TVL'] / 1000000000
            combined_pools_tvl_top_10.index = combined_pools_tvl_top_10['Symbol']
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['Symbol'], axis=1)
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.round(2)

            print(combined_pools_tvl_top_10)

            return combined_pools_tvl_top_10

        elif 'APY' in message_content:

            ##get the top 10 by apy
            combined_pools_apy_top_10 = combined_pools.sort_values('APY',
                                                                   ascending=False)  ###can change this based on what timeframe we looking at
            combined_pools_apy_top_10 = combined_pools_apy_top_10.head(10)
            combined_pools_apy_top_10 = combined_pools_apy_top_10[
                ['Pool', 'Symbol', 'Project', 'Chain', 'TVL', 'APY', 'APY 7D', 'APY 30D']]
            print(combined_pools_apy_top_10)

            pool_names = list(combined_pools_apy_top_10['Pool'])

            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['Pool'], axis=1)

            ####MAY WANNA ADD TVL/APY TREND OVER LAST 30 DAYS

            ###Get data for all pools and save as excel file

            tvl_2d_change = {}
            tvl_7d_change = {}
            tvl_30d_change = {}

            apy_30d_mean = {}

            for i in pool_names:
                hist_data = requests.get(f'https://yields.llama.fi/chart/{i}').json()
                hist_tvl = []
                hist_apy = []
                for j in range(len(hist_data['data'])):
                    hist_tvl.append(hist_data['data'][j]['tvlUsd'])
                    hist_apy.append(hist_data['data'][j]['apy'])
                try:
                    tvl_2d_change[i] = (hist_tvl[-1] / hist_tvl[-3]) - 1
                except:
                    tvl_2d_change[i] = np.nan

                try:
                    tvl_7d_change[i] = (hist_tvl[-1] / hist_tvl[-8]) - 1
                except:
                    tvl_7d_change[i] = np.nan
                try:
                    tvl_30d_change[i] = (hist_tvl[-1] / hist_tvl[-31]) - 1
                except:
                    tvl_30d_change[i] = np.nan

                try:
                    apy_30d_mean[i] = (pd.Series(hist_apy).rolling(30).mean().iloc[-1])
                except:
                    apy_30d_mean[i] = np.nan

            print(tvl_7d_change)

            tvl_2d_change_df = pd.DataFrame.from_dict(tvl_2d_change, orient='index')

            tvl_7d_change_df = pd.DataFrame.from_dict(tvl_7d_change, orient='index')
            tvl_30d_change_df = pd.DataFrame.from_dict(tvl_30d_change, orient='index')
            apy_30d_mean_df = pd.DataFrame.from_dict(apy_30d_mean, orient='index')

            tvl_2d_change_df.columns = ['TVL 2D %']

            tvl_7d_change_df.columns = ['TVL 7D %']
            tvl_30d_change_df.columns = ['TVL 30D %']
            apy_30d_mean_df.columns = ['APY 30D M']

            tvl_2d_change_df = tvl_2d_change_df.reset_index()
            tvl_2d_change_df = tvl_2d_change_df.drop(['index'], axis=1)

            tvl_7d_change_df = tvl_7d_change_df.reset_index()
            tvl_7d_change_df = tvl_7d_change_df.drop(['index'], axis=1)

            tvl_30d_change_df = tvl_30d_change_df.reset_index()
            tvl_30d_change_df = tvl_30d_change_df.drop(['index'], axis=1)

            apy_30d_mean_df = apy_30d_mean_df.reset_index()
            apy_30d_mean_df = apy_30d_mean_df.drop(['index'], axis=1)

            combined_pools_apy_top_10 = combined_pools_apy_top_10.reset_index()
            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['index'], axis=1)

            combined_pools_apy_top_10 = pd.concat(
                [combined_pools_apy_top_10, tvl_2d_change_df, tvl_7d_change_df, tvl_30d_change_df, apy_30d_mean_df],
                axis=1)

            combined_pools_apy_top_10 = combined_pools_apy_top_10.reset_index()
            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['index'], axis=1)

            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['APY 7D', 'APY 30D', 'APY 30D M'], axis=1)
            combined_pools_apy_top_10.index = combined_pools_apy_top_10['Symbol']
            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['Symbol'], axis=1)
            combined_pools_apy_top_10 = combined_pools_apy_top_10.round(2)
            print(combined_pools_apy_top_10)

            print(combined_pools_apy_top_10)

            return combined_pools_apy_top_10

    elif message_content.startswith('STABLE_POOL_STATS'):

        pools_data = requests.get('https://yields.llama.fi/pools')
        pools_data = pools_data.json()

        tvl_value = []
        pool_name = []
        chain_name = []
        current_apy = []
        pool_symbol = []
        apy_1d = []
        apy_7d = []
        apy_30d = []
        stable = []
        project = []

        for i in range(len(pools_data['data'])):
            tvl_value.append(pools_data['data'][i]['tvlUsd'])
            pool_name.append(pools_data['data'][i]['pool'])
            chain_name.append(pools_data['data'][i]['chain'])
            pool_symbol.append(pools_data['data'][i]['symbol'])
            current_apy.append(pools_data['data'][i]['apy'])
            apy_1d.append(pools_data['data'][i]['apyPct1D'])
            apy_7d.append(pools_data['data'][i]['apyPct7D'])
            apy_30d.append(pools_data['data'][i]['apyPct30D'])
            stable.append(pools_data['data'][i]['stablecoin'])
            project.append(pools_data['data'][i]['project'])

        tvl_value = pd.DataFrame(tvl_value)
        pool_name = pd.DataFrame(pool_name)
        chain_name = pd.DataFrame(chain_name)
        pool_symbol = pd.DataFrame(pool_symbol)
        current_apy = pd.DataFrame(current_apy)
        apy_1d = pd.DataFrame(apy_1d)
        apy_7d = pd.DataFrame(apy_7d)
        apy_30d = pd.DataFrame(apy_30d)
        stable = pd.DataFrame(stable)
        project = pd.DataFrame(project)

        combined_pools = pd.concat(
            [chain_name, pool_name, tvl_value, pool_symbol, current_apy, apy_1d, apy_7d, apy_30d, project, stable],
            axis=1)

        combined_pools.columns = ['Chain', 'Pool', 'TVL', 'Symbol', 'APY', 'APY 1D', 'APY 7D', 'APY 30D', 'Project',
                                  'Stable']

        stable_all_pools = combined_pools[combined_pools['Stable'] == True]
        print(stable_all_pools)

        stable_all_pools = stable_all_pools.drop(['Stable'], axis=1)

        if 'TVL' in message_content:
            #####Rank stable pools by TVL

            combined_pools_tvl_top_10 = stable_all_pools.sort_values('TVL',
                                                                     ascending=False)  ###can change this based on what timeframe we looking at
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.head(10)
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10[
                ['Pool', 'Symbol', 'Project', 'Chain', 'TVL', 'APY', 'APY 7D', 'APY 30D']]

            pool_names = list(combined_pools_tvl_top_10['Pool'])

            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['Pool'], axis=1)

            ####MAY WANNA ADD TVL/APY TREND OVER LAST 30 DAYS

            ###Get data for all pools and save as excel file

            tvl_7d_change = {}
            tvl_30d_change = {}

            apy_30d_mean = {}

            for i in pool_names:
                hist_data = requests.get(f'https://yields.llama.fi/chart/{i}').json()
                hist_tvl = []
                hist_apy = []
                for j in range(len(hist_data['data'])):
                    hist_tvl.append(hist_data['data'][j]['tvlUsd'])
                    hist_apy.append(hist_data['data'][j]['apy'])

                try:
                    tvl_7d_change[i] = (hist_tvl[-1] / hist_tvl[-8]) - 1
                except:
                    tvl_7d_change[i] = np.nan
                try:
                    tvl_30d_change[i] = (hist_tvl[-1] / hist_tvl[-31]) - 1
                except:
                    tvl_30d_change[i] = np.nan
                try:
                    apy_30d_mean[i] = (pd.Series(hist_apy).rolling(30).mean().iloc[-1])
                except:
                    apy_30d_mean[i] = np.nan

            tvl_7d_change_df = pd.DataFrame.from_dict(tvl_7d_change, orient='index')
            tvl_30d_change_df = pd.DataFrame.from_dict(tvl_30d_change, orient='index')
            apy_30d_mean_df = pd.DataFrame.from_dict(apy_30d_mean, orient='index')

            tvl_7d_change_df.columns = ['TVL 7D %']
            tvl_30d_change_df.columns = ['TVL 30D %']
            apy_30d_mean_df.columns = ['APY 30D Mean']

            tvl_7d_change_df = tvl_7d_change_df.reset_index()
            tvl_7d_change_df = tvl_7d_change_df.drop(['index'], axis=1)

            tvl_30d_change_df = tvl_30d_change_df.reset_index()
            tvl_30d_change_df = tvl_30d_change_df.drop(['index'], axis=1)

            apy_30d_mean_df = apy_30d_mean_df.reset_index()
            apy_30d_mean_df = apy_30d_mean_df.drop(['index'], axis=1)

            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.reset_index()
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['index'], axis=1)

            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.reset_index()
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['index'], axis=1)

            combined_pools_tvl_top_10 = pd.concat(
                [combined_pools_tvl_top_10, tvl_7d_change_df, tvl_30d_change_df, apy_30d_mean_df], axis=1)
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['APY 30D'], axis=1)
            combined_pools_tvl_top_10['TVL'] = combined_pools_tvl_top_10['TVL'] / 1000000000
            combined_pools_tvl_top_10.index = combined_pools_tvl_top_10['Symbol']
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.drop(['Symbol'], axis=1)
            combined_pools_tvl_top_10 = combined_pools_tvl_top_10.round(2)

            return combined_pools_tvl_top_10

        elif 'APY' in message_content:

            ##get the top 10 by apy
            combined_pools_apy_top_10 = stable_all_pools.sort_values('APY',
                                                                     ascending=False)  ###can change this based on what timeframe we looking at
            combined_pools_apy_top_10 = combined_pools_apy_top_10.head(10)
            combined_pools_apy_top_10 = combined_pools_apy_top_10[
                ['Pool', 'Symbol', 'Project', 'Chain', 'TVL', 'APY', 'APY 7D', 'APY 30D']]
            print(combined_pools_apy_top_10)

            pool_names = list(combined_pools_apy_top_10['Pool'])

            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['Pool'], axis=1)

            ####MAY WANNA ADD TVL/APY TREND OVER LAST 30 DAYS

            ###Get data for all pools and save as excel file

            tvl_7d_change = {}
            tvl_30d_change = {}

            apy_30d_mean = {}

            for i in pool_names:
                hist_data = requests.get(f'https://yields.llama.fi/chart/{i}').json()
                hist_tvl = []
                hist_apy = []
                for j in range(len(hist_data['data'])):
                    hist_tvl.append(hist_data['data'][j]['tvlUsd'])
                    hist_apy.append(hist_data['data'][j]['apy'])
                try:
                    tvl_7d_change[i] = (hist_tvl[-1] / hist_tvl[-8]) - 1
                except:
                    tvl_7d_change[i] = np.nan
                try:
                    tvl_30d_change[i] = (hist_tvl[-1] / hist_tvl[-31]) - 1
                except:
                    tvl_30d_change[i] = np.nan

                try:
                    apy_30d_mean[i] = (pd.Series(hist_apy).rolling(30).mean().iloc[-1])
                except:
                    apy_30d_mean[i] = np.nan

            print(tvl_7d_change)

            tvl_7d_change_df = pd.DataFrame.from_dict(tvl_7d_change, orient='index')
            tvl_30d_change_df = pd.DataFrame.from_dict(tvl_30d_change, orient='index')
            apy_30d_mean_df = pd.DataFrame.from_dict(apy_30d_mean, orient='index')

            tvl_7d_change_df.columns = ['TVL 7D %']
            tvl_30d_change_df.columns = ['TVL 30D %']
            apy_30d_mean_df.columns = ['APY 30D M']

            tvl_7d_change_df = tvl_7d_change_df.reset_index()
            tvl_7d_change_df = tvl_7d_change_df.drop(['index'], axis=1)

            tvl_30d_change_df = tvl_30d_change_df.reset_index()
            tvl_30d_change_df = tvl_30d_change_df.drop(['index'], axis=1)

            apy_30d_mean_df = apy_30d_mean_df.reset_index()
            apy_30d_mean_df = apy_30d_mean_df.drop(['index'], axis=1)

            combined_pools_apy_top_10 = combined_pools_apy_top_10.reset_index()
            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['index'], axis=1)

            combined_pools_apy_top_10 = pd.concat(
                [combined_pools_apy_top_10, tvl_7d_change_df, tvl_30d_change_df, apy_30d_mean_df], axis=1)
            print(combined_pools_apy_top_10)

            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['APY 7D', 'APY 30D', 'APY 30D M'], axis=1)
            combined_pools_apy_top_10.index = combined_pools_apy_top_10['Symbol']
            combined_pools_apy_top_10 = combined_pools_apy_top_10.drop(['Symbol'], axis=1)
            combined_pools_apy_top_10 = combined_pools_apy_top_10.round(2)
            print(combined_pools_apy_top_10)

            return combined_pools_apy_top_10

