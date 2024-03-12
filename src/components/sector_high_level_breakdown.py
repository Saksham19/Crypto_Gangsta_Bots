import requests
import json
import pandas as pd
import datetime
import os
from dotenv import load_dotenv

load_dotenv()


######DISCORD STUFFF

# load_dotenv()
# TOKEN = os.getenv('DISCORD_TOKEN')
# GUILD = os.getenv('DISCORD_GUILD')

TOKEN = 'MTA3MTQzODI2Njk4Mjc0NDA3NA.GEWzSL.TNDkxPc6PZxX6hsiKZGlIxUzYk9v-tgSmNV-Rg'
GUILD = "Saksham1212's server"

cmc_api_key = os.getenv('CMC_API_KEY')


def sector_bot(message_content):
        api_key = cmc_api_key

        headers = {
          'Accepts': 'application/json',
          'X-CMC_PRO_API_KEY': f'{api_key}',
        }
        res = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/categories',headers=headers)
        data = json.loads(res.text)



        ###lets pull out avg_price_change and market_cap_change to rank these

        category_id = []
        category_name = []
        category_avg_price_change = []
        category_avg_mcap_change = []
        category_avg_vol_change = []

        for i in range(len(data['data'])):

            category_id.append(data['data'][i]['id'])
            category_name.append(data['data'][i]['name'])
            try:
                category_avg_price_change.append(data['data'][i]['avg_price_change'])
            except:
                category_avg_price_change.append('N/A')
            try:
                category_avg_mcap_change.append(data['data'][i]['market_cap_change'])
            except:
                category_avg_mcap_change.append('N/A')
            try:
                category_avg_vol_change.append(data['data'][i]['volume_change'])
            except:
                category_avg_vol_change.append('N/A')

        category_all_data = pd.concat([pd.DataFrame(category_id),pd.DataFrame(category_name),pd.DataFrame(category_avg_price_change),pd.DataFrame(category_avg_mcap_change),pd.DataFrame(category_avg_vol_change)],axis=1)
        category_all_data.columns = ['ID','NAME','AVG_PRICE_CHANGE','AVG_MCAP_CHANGE','AVG_VOL_CHANGE']
        category_all_data = category_all_data.replace('N/A', pd.NA).dropna()

        category_all_data['AVG_PRICE_CHANGE'] = category_all_data['AVG_PRICE_CHANGE'].astype('float')

        ##first, we get global crypto volumes data

        vol_data = requests.get('https://min-api.cryptocompare.com/data/exchange/histoday?tsym=USD&limit=60')
        vol_data = vol_data.json()

        date_value = []
        vol_value = []

        for i in range(len(vol_data['Data'])):
            date_value.append(datetime.datetime.utcfromtimestamp(vol_data['Data'][i]['time']).strftime('%d-%m-%Y'))
            vol_value.append(vol_data['Data'][i]['volume'])

        vol_data_all = vol_value

        ####indivdual coin volumes

        ######add in btc and eth volumes separately

        btc_eth_token = ['BTC', 'ETH']

        btc_eth_vol_data_df = pd.DataFrame()

        for token in btc_eth_token:
            token_volume_data = requests.get(
                f'https://min-api.cryptocompare.com/data/symbol/histoday?fsym={token}&tsym=USD&limit=60')
            token_volume_data = token_volume_data.json()

            token_vol_value = []

            for i in range(len(token_volume_data['Data'])):
                token_vol_value.append(token_volume_data['Data'][i]['total_volume_total'])

            btc_eth_vol_data_df = pd.concat([btc_eth_vol_data_df, pd.DataFrame(token_vol_value)], axis=1)


    ####VOL DATA BTC + ETH
        print(btc_eth_vol_data_df)

        print(vol_data_all)

        btc_eth_vol_data_df.columns = ['BTC', 'ETH']

        btc_vol_data_list = list(btc_eth_vol_data_df['BTC'])
        eth_vol_data_list = list(btc_eth_vol_data_df['ETH'])

        btc_vol_percent = [a / b for a, b in zip(btc_vol_data_list, vol_data_all)]
        btc_vol_percent = [a * 100 for a in btc_vol_percent]

        eth_vol_percent = [a / b for a, b in zip(eth_vol_data_list, vol_data_all)]
        eth_vol_percent = [a * 100 for a in eth_vol_percent]

        btc_eth_vol_data_df = pd.concat([pd.DataFrame(btc_vol_percent), pd.DataFrame(eth_vol_percent)], axis=1)
        btc_eth_vol_data_df.columns = ['BTC', 'ETH']

        if 'PRICE' in message_content:


            category_all_data= category_all_data.sort_values(by='AVG_PRICE_CHANGE', ascending=False)

            category_ids = category_all_data['ID']
            category_all_data = category_all_data.drop(['ID'],axis=1)


            category_all_data = category_all_data.head(20)

            cat_10 = category_all_data.iloc[:10]
            cat_20 = category_all_data.iloc[10:20]


            category_vol_percent_combined = pd.DataFrame()

            category_selected_id = category_ids[:20]

            for category in category_selected_id:
                params = {'id': f'{category}'}
                ######category tokens

                res = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/category', headers=headers,
                                   params=params)
                data = json.loads(res.text)

                category_name = data['data']['name']
                category_coins = []
                percent_change_1h = []
                volume_change_24h = []
                percent_change_24h = []
                percent_change_7d = []
                percent_change_30d = []
                percent_change_60d = []

                for i in range(len(data['data']['coins'])):
                    category_coins.append(data['data']['coins'][i]['symbol'])
                    percent_change_1h.append(data['data']['coins'][i]['quote']['USD']['percent_change_1h'])
                    percent_change_24h.append(data['data']['coins'][i]['quote']['USD']['percent_change_24h'])
                    percent_change_7d.append(data['data']['coins'][i]['quote']['USD']['percent_change_7d'])
                    percent_change_30d.append(data['data']['coins'][i]['quote']['USD']['percent_change_30d'])
                    percent_change_60d.append(data['data']['coins'][i]['quote']['USD']['percent_change_60d'])
                    volume_change_24h.append(data['data']['coins'][i]['quote']['USD']['volume_change_24h'])

                category_coins_df = pd.concat(
                    [pd.DataFrame(category_coins), pd.DataFrame(volume_change_24h), pd.DataFrame(percent_change_1h),
                     pd.DataFrame(percent_change_24h)
                        , pd.DataFrame(percent_change_7d), pd.DataFrame(percent_change_30d),
                     pd.DataFrame(percent_change_60d)], axis=1)
                category_coins_df.columns = ['NAME', 'VOLUME_CHG_24H', 'PRICE_CHG_1H', 'PRICE_CHG_24H', 'PRICE_CHG_7D',
                                             'PRICE_CHG_30D', 'PRICE_CHG_60D']
                category_coins_df = category_coins_df.sort_values(by='VOLUME_CHG_24H', ascending=False)

                top_5_tokens = category_coins_df['NAME'].head(10).values

                print(top_5_tokens)

                ###maybe stick with top 5 tokens per sector?

                token_vol_data_df = pd.DataFrame()

                try:
                    for token in (top_5_tokens):

                        token_volume_data = requests.get(
                            f'https://min-api.cryptocompare.com/data/symbol/histoday?fsym={token}&tsym=USD&limit=60')
                        token_volume_data = token_volume_data.json()

                        token_vol_value = []

                        try:
                            for i in range(len(token_volume_data['Data'])):
                                token_vol_value.append(token_volume_data['Data'][i]['total_volume_total'])

                        except:
                            pass

                        token_vol_data_df = pd.concat([token_vol_data_df, pd.DataFrame(token_vol_value)], axis=1)

                    token_vol_data_df['SUM'] = token_vol_data_df.sum(axis=1)

                    token_vol_data_list = list(token_vol_data_df['SUM'])

                    token_vol_percent = [a / b for a, b in zip(token_vol_data_list, vol_data_all)]
                    token_vol_percent = [a * 100 for a in token_vol_percent]

                    token_vol_percent_df = pd.DataFrame(token_vol_percent)
                    token_vol_percent_df.columns = [f'{category_name}']

                except:
                    pass

                category_vol_percent_combined = pd.concat([category_vol_percent_combined, token_vol_percent_df], axis=1)

            category_vol_percent_combined = category_vol_percent_combined
            print(category_vol_percent_combined)

            #####ADD in BTC + ETH Vol here

            category_vol_percent_combined = pd.concat([category_vol_percent_combined, btc_eth_vol_data_df], axis=1)
            category_vol_percent_combined.index = pd.to_datetime(date_value, dayfirst=True)
            print(category_vol_percent_combined)

            ###Normalize this 100%

            # category_vol_percent_combined = category_vol_percent_combined.drop(['BTC'],axis=1)

            category_vol_percent_combined = (
                        category_vol_percent_combined.div(category_vol_percent_combined.sum(axis=1),
                                                          axis=0) * 100).round(2)
            category_vol_percent_combined.index = pd.to_datetime(category_vol_percent_combined.index).strftime('%d-%m')
            print(category_vol_percent_combined)

            latest_value = category_vol_percent_combined.iloc[-1]
            value_7d_ago = category_vol_percent_combined.iloc[-8] if len(
                category_vol_percent_combined) >= 8 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                   index=category_vol_percent_combined.columns)
            value_14d_ago = category_vol_percent_combined.iloc[-15] if len(
                category_vol_percent_combined) >= 15 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                    index=category_vol_percent_combined.columns)
            value_30d_ago = category_vol_percent_combined.iloc[-31] if len(
                category_vol_percent_combined) >= 31 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                    index=category_vol_percent_combined.columns)

            print(latest_value)
            print(value_7d_ago)

            combined_sector_df = pd.concat([latest_value, value_7d_ago, value_14d_ago, value_30d_ago], axis=1)
            combined_sector_df.columns = ['1D', '7D', '14D', '30D']

            print(combined_sector_df)

            return cat_10,cat_20,combined_sector_df

        elif 'MCAP' in message_content:

            category_all_data = category_all_data.sort_values(by='AVG_MCAP_CHANGE', ascending=False)

            category_ids = category_all_data['ID']

            category_all_data = category_all_data.drop(['ID'], axis=1)

            category_all_data = category_all_data.head(20)

            cat_10 = category_all_data.iloc[:10]
            cat_20 = category_all_data.iloc[10:20]


            category_vol_percent_combined = pd.DataFrame()

            category_selected_id = category_ids[:20]

            for category in category_selected_id:
                params = {'id': f'{category}'}
                ######category tokens

                res = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/category', headers=headers,
                                   params=params)
                data = json.loads(res.text)

                category_name = data['data']['name']
                category_coins = []
                percent_change_1h = []
                volume_change_24h = []
                percent_change_24h = []
                percent_change_7d = []
                percent_change_30d = []
                percent_change_60d = []

                for i in range(len(data['data']['coins'])):
                    category_coins.append(data['data']['coins'][i]['symbol'])
                    percent_change_1h.append(data['data']['coins'][i]['quote']['USD']['percent_change_1h'])
                    percent_change_24h.append(data['data']['coins'][i]['quote']['USD']['percent_change_24h'])
                    percent_change_7d.append(data['data']['coins'][i]['quote']['USD']['percent_change_7d'])
                    percent_change_30d.append(data['data']['coins'][i]['quote']['USD']['percent_change_30d'])
                    percent_change_60d.append(data['data']['coins'][i]['quote']['USD']['percent_change_60d'])
                    volume_change_24h.append(data['data']['coins'][i]['quote']['USD']['volume_change_24h'])

                category_coins_df = pd.concat(
                    [pd.DataFrame(category_coins), pd.DataFrame(volume_change_24h), pd.DataFrame(percent_change_1h),
                     pd.DataFrame(percent_change_24h)
                        , pd.DataFrame(percent_change_7d), pd.DataFrame(percent_change_30d),
                     pd.DataFrame(percent_change_60d)], axis=1)
                category_coins_df.columns = ['NAME', 'VOLUME_CHG_24H', 'PRICE_CHG_1H', 'PRICE_CHG_24H', 'PRICE_CHG_7D',
                                             'PRICE_CHG_30D', 'PRICE_CHG_60D']
                category_coins_df = category_coins_df.sort_values(by='VOLUME_CHG_24H', ascending=False)

                top_5_tokens = category_coins_df['NAME'].head(10).values

                print(top_5_tokens)

                ###maybe stick with top 5 tokens per sector?

                token_vol_data_df = pd.DataFrame()

                try:
                    for token in (top_5_tokens):

                        token_volume_data = requests.get(
                            f'https://min-api.cryptocompare.com/data/symbol/histoday?fsym={token}&tsym=USD&limit=60')
                        token_volume_data = token_volume_data.json()

                        token_vol_value = []

                        try:
                            for i in range(len(token_volume_data['Data'])):
                                token_vol_value.append(token_volume_data['Data'][i]['total_volume_total'])

                        except:
                            pass

                        token_vol_data_df = pd.concat([token_vol_data_df, pd.DataFrame(token_vol_value)], axis=1)

                    token_vol_data_df['SUM'] = token_vol_data_df.sum(axis=1)

                    token_vol_data_list = list(token_vol_data_df['SUM'])

                    token_vol_percent = [a / b for a, b in zip(token_vol_data_list, vol_data_all)]
                    token_vol_percent = [a * 100 for a in token_vol_percent]

                    token_vol_percent_df = pd.DataFrame(token_vol_percent)
                    token_vol_percent_df.columns = [f'{category_name}']

                except:
                    pass

                category_vol_percent_combined = pd.concat([category_vol_percent_combined, token_vol_percent_df], axis=1)

            category_vol_percent_combined = category_vol_percent_combined
            print(category_vol_percent_combined)

            #####ADD in BTC + ETH Vol here

            category_vol_percent_combined = pd.concat([category_vol_percent_combined, btc_eth_vol_data_df], axis=1)
            category_vol_percent_combined.index = pd.to_datetime(date_value, dayfirst=True)
            print(category_vol_percent_combined)

            ###Normalize this 100%

            # category_vol_percent_combined = category_vol_percent_combined.drop(['BTC'],axis=1)

            category_vol_percent_combined = (
                        category_vol_percent_combined.div(category_vol_percent_combined.sum(axis=1),
                                                          axis=0) * 100).round(2)
            category_vol_percent_combined.index = pd.to_datetime(category_vol_percent_combined.index).strftime('%d-%m')
            print(category_vol_percent_combined)

            latest_value = category_vol_percent_combined.iloc[-1]
            value_7d_ago = category_vol_percent_combined.iloc[-8] if len(
                category_vol_percent_combined) >= 8 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                   index=category_vol_percent_combined.columns)
            value_14d_ago = category_vol_percent_combined.iloc[-15] if len(
                category_vol_percent_combined) >= 15 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                    index=category_vol_percent_combined.columns)
            value_30d_ago = category_vol_percent_combined.iloc[-31] if len(
                category_vol_percent_combined) >= 31 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                    index=category_vol_percent_combined.columns)

            print(latest_value)
            print(value_7d_ago)

            combined_sector_df = pd.concat([latest_value, value_7d_ago, value_14d_ago, value_30d_ago], axis=1)
            combined_sector_df.columns = ['1D', '7D', '14D', '30D']

            print(combined_sector_df)

            return cat_10,cat_20,combined_sector_df



        elif 'VOL' in message_content:

            category_all_data = category_all_data.sort_values(by='AVG_VOL_CHANGE', ascending=False)
            category_ids = category_all_data['ID']

            category_all_data = category_all_data.drop(['ID'], axis=1)

            category_all_data = category_all_data.head(20)


            category_vol_percent_combined = pd.DataFrame()

            category_selected_id = category_ids[:20]

            for category in category_selected_id:
                params = {'id': f'{category}'}
                ######category tokens

                res = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/category', headers=headers,
                                   params=params)
                data = json.loads(res.text)

                category_name = data['data']['name']
                category_coins = []
                percent_change_1h = []
                volume_change_24h = []
                percent_change_24h = []
                percent_change_7d = []
                percent_change_30d = []
                percent_change_60d = []

                for i in range(len(data['data']['coins'])):
                    category_coins.append(data['data']['coins'][i]['symbol'])
                    percent_change_1h.append(data['data']['coins'][i]['quote']['USD']['percent_change_1h'])
                    percent_change_24h.append(data['data']['coins'][i]['quote']['USD']['percent_change_24h'])
                    percent_change_7d.append(data['data']['coins'][i]['quote']['USD']['percent_change_7d'])
                    percent_change_30d.append(data['data']['coins'][i]['quote']['USD']['percent_change_30d'])
                    percent_change_60d.append(data['data']['coins'][i]['quote']['USD']['percent_change_60d'])
                    volume_change_24h.append(data['data']['coins'][i]['quote']['USD']['volume_change_24h'])

                category_coins_df = pd.concat(
                    [pd.DataFrame(category_coins), pd.DataFrame(volume_change_24h), pd.DataFrame(percent_change_1h),
                     pd.DataFrame(percent_change_24h)
                        , pd.DataFrame(percent_change_7d), pd.DataFrame(percent_change_30d),
                     pd.DataFrame(percent_change_60d)], axis=1)
                category_coins_df.columns = ['NAME', 'VOLUME_CHG_24H', 'PRICE_CHG_1H', 'PRICE_CHG_24H', 'PRICE_CHG_7D',
                                             'PRICE_CHG_30D', 'PRICE_CHG_60D']
                category_coins_df = category_coins_df.sort_values(by='VOLUME_CHG_24H', ascending=False)

                top_5_tokens = category_coins_df['NAME'].head(10).values

                print(top_5_tokens)

                ###maybe stick with top 5 tokens per sector?

                token_vol_data_df = pd.DataFrame()

                try:
                    for token in (top_5_tokens):

                        token_volume_data = requests.get(
                            f'https://min-api.cryptocompare.com/data/symbol/histoday?fsym={token}&tsym=USD&limit=60')
                        token_volume_data = token_volume_data.json()

                        token_vol_value = []

                        try:
                            for i in range(len(token_volume_data['Data'])):
                                token_vol_value.append(token_volume_data['Data'][i]['total_volume_total'])

                        except:
                            pass

                        token_vol_data_df = pd.concat([token_vol_data_df, pd.DataFrame(token_vol_value)], axis=1)

                    token_vol_data_df['SUM'] = token_vol_data_df.sum(axis=1)

                    token_vol_data_list = list(token_vol_data_df['SUM'])

                    token_vol_percent = [a / b for a, b in zip(token_vol_data_list, vol_data_all)]
                    token_vol_percent = [a * 100 for a in token_vol_percent]

                    token_vol_percent_df = pd.DataFrame(token_vol_percent)
                    token_vol_percent_df.columns = [f'{category_name}']

                except:
                    pass

                category_vol_percent_combined = pd.concat([category_vol_percent_combined, token_vol_percent_df], axis=1)

            category_vol_percent_combined = category_vol_percent_combined
            print(category_vol_percent_combined)

            #####ADD in BTC + ETH Vol here

            category_vol_percent_combined = pd.concat([category_vol_percent_combined, btc_eth_vol_data_df], axis=1)
            category_vol_percent_combined.index = pd.to_datetime(date_value, dayfirst=True)
            print(category_vol_percent_combined)

            ###Normalize this 100%

            # category_vol_percent_combined = category_vol_percent_combined.drop(['BTC'],axis=1)

            category_vol_percent_combined = (
                    category_vol_percent_combined.div(category_vol_percent_combined.sum(axis=1),
                                                      axis=0) * 100).round(2)
            category_vol_percent_combined.index = pd.to_datetime(category_vol_percent_combined.index).strftime('%d-%m')
            print(category_vol_percent_combined)

            latest_value = category_vol_percent_combined.iloc[-1]
            value_7d_ago = category_vol_percent_combined.iloc[-8] if len(
                category_vol_percent_combined) >= 8 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                   index=category_vol_percent_combined.columns)
            value_14d_ago = category_vol_percent_combined.iloc[-15] if len(
                category_vol_percent_combined) >= 15 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                    index=category_vol_percent_combined.columns)
            value_30d_ago = category_vol_percent_combined.iloc[-31] if len(
                category_vol_percent_combined) >= 31 else pd.Series([0] * len(category_vol_percent_combined.columns),
                                                                    index=category_vol_percent_combined.columns)

            print(latest_value)
            print(value_7d_ago)

            combined_sector_df = pd.concat([latest_value, value_7d_ago, value_14d_ago, value_30d_ago], axis=1)
            combined_sector_df.columns = ['1D', '7D', '14D', '30D']

            print(combined_sector_df)

            return combined_sector_df,cat_10,cat_20

