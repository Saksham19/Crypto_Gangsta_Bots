import os

import requests
import json
import pandas as pd
from flipside import Flipside
import datetime
from dotenv import load_dotenv

load_dotenv()

cmc_api_key = os.getenv('CMC_API_KEY')
arkham_api_key = os.getenv('ARKHAM_API_KEY')
flipside_api_key = os.getenv('FLIPSIDE_API_KEY')

def specific_sector_bot(message_content):

        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': f'{cmc_api_key}',
        }
        res = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/categories', headers=headers)
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

        category_all_data = pd.concat(
            [pd.DataFrame(category_id), pd.DataFrame(category_name), pd.DataFrame(category_avg_price_change),
             pd.DataFrame(category_avg_mcap_change), pd.DataFrame(category_avg_vol_change)], axis=1)
        category_all_data.columns = ['ID', 'NAME', 'AVG_PRICE_CHANGE', 'AVG_MCAP_CHANGE', 'AVG_VOL_CHANGE']
        category_all_data = category_all_data.replace('N/A', pd.NA).dropna()

        category_input_name = message_content

        print(category_input_name)

        selected_category = category_all_data[category_all_data.NAME == f'{category_input_name}']['ID'].values[0]
        print(selected_category)

        ###


        params = {'id': f'{selected_category}'}

        res = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/category', headers=headers, params=params)
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
                , pd.DataFrame(percent_change_7d), pd.DataFrame(percent_change_30d), pd.DataFrame(percent_change_60d)], axis=1)
        category_coins_df.columns = ['NAME', 'VOLUME_CHG_24H', 'PRICE_CHG_1H', 'PRICE_CHG_24H', 'PRICE_CHG_7D', 'PRICE_CHG_30D',
                                     'PRICE_CHG_60D']

        ####CAN SPECIFY IN DISCORD  - SORT BY  PRICE/VOLUME
        category_coins_df = category_coins_df.sort_values(by='VOLUME_CHG_24H', ascending=False).head(10)

        top_5_tokens = category_coins_df['NAME'].values

        print(top_5_tokens)

        ##############VOL CHANGE


        token_vol_data_df = pd.DataFrame()

        try:
            for token in (top_5_tokens):

                token_volume_data = requests.get(
                    f'https://min-api.cryptocompare.com/data/symbol/histoday?fsym={token}&tsym=USD&limit=60')
                token_volume_data = token_volume_data.json()

                token_vol_value = []

                for i in range(len(token_volume_data['Data'])):
                    token_vol_value.append(token_volume_data['Data'][i]['total_volume_total'])
                token_vol_data_df[f'{token}'] = token_vol_value




        except:
            for token in (top_5_tokens):

                token_volume_data = requests.get(
                    f'https://min-api.cryptocompare.com/data/symbol/histoday?fsym={token}&tsym=USDT&limit=60')
                token_volume_data = token_volume_data.json()

                token_vol_value = []

                try:
                    for i in range(len(token_volume_data['Data'])):
                        token_vol_value.append(token_volume_data['Data'][i]['total_volume_total'])

                except:
                    pass

                try:
                    token_vol_data_df[f'{token}'] = token_vol_value
                except:
                    print(f'no vol data for {token}')

        token_vol_data_df = token_vol_data_df.dropna(how='all', axis=1)

        print(token_vol_data_df)

        ###Aggregated volume -

        token_vol_data_df = token_vol_data_df.div(token_vol_data_df.sum(axis=1), axis=0)

        latest_value = token_vol_data_df.iloc[-1]
        value_7d_ago = token_vol_data_df.iloc[-8] if len(token_vol_data_df) >= 8 else pd.Series(
            [0] * len(token_vol_data_df.columns), index=token_vol_data_df.columns)
        value_14d_ago = token_vol_data_df.iloc[-15] if len(token_vol_data_df) >= 15 else pd.Series(
            [0] * len(token_vol_data_df.columns), index=token_vol_data_df.columns)
        value_30d_ago = token_vol_data_df.iloc[-31] if len(token_vol_data_df) >= 31 else pd.Series(
            [0] * len(token_vol_data_df.columns), index=token_vol_data_df.columns)

        combined_sector_df = pd.concat([latest_value, value_7d_ago, value_14d_ago, value_30d_ago], axis=1)
        combined_sector_df.columns = ['1D', '7D', '14D', '30D']
        combined_sector_df = combined_sector_df*100
        print(combined_sector_df)


        ######WALLET STUFF

        #####pulling in the wallet addresses here


        top_5_tokens = top_5_tokens[:5]

        """Initialize Flipside with your API Key / API Url"""

        token_addr_data = {}

        flipside = Flipside(f'{flipside_api_key}', "https://api-v2.flipsidecrypto.xyz")

        for token_name in top_5_tokens:

            sql = f"""WITH whale AS( SELECT b.last_activity_block_timestamp as day, b.USER_ADDRESS as whale, b.SYMBOL as symbol, b.CURRENT_BAL as amount, b.USD_VALUE_NOW as amount_usd FROM ethereum.core.ez_current_balances b WHERE b.symbol in ( '{token_name}') AND b.last_activity_block_timestamp::date >= '2023-01-01' GROUP BY b.last_activity_block_timestamp, b.user_address, b.symbol, b.current_bal, b.usd_value_now ORDER BY b.current_bal DESC ), label as ( SELECT address, address_name as name, label_type, label_subtype, label FROM ethereum.core.dim_labels ), data as ( SELECT a.whale as whale, max(l.name) as Label, a.symbol as Symbol, a.amount,a.amount_usd as Amount_USD1 FROM whale a LEFT JOIN label l on l.address = a.whale WHERE a.amount_usd > 50000  AND Label IS NULL GROUP BY 1,3,4,5 ORDER BY a.amount_usd DESC) SELECT * from data ORDER BY 4 DESC LIMIT 10"""
            """Run the query against Flipside's query engine and await the results"""

            query_result_set = flipside.query(sql).records

            ###need to filter for 'label == null' only

            whale_col = []
            label_col = []
            symbol_col = []
            amt_col = []
            amt_usd_col = []

            try:

                for i in range(len(query_result_set)):
                    whale_col.append(query_result_set[i]['whale'])
                    label_col.append(query_result_set[i]['label'])
                    symbol_col.append(query_result_set[i]['symbol'])
                    amt_col.append(query_result_set[i]['amount'])
                    amt_usd_col.append(query_result_set[i]['amount_usd1'])

                combined_df = pd.concat(
                    [pd.DataFrame(whale_col), pd.DataFrame(label_col), pd.DataFrame(symbol_col), pd.DataFrame(amt_col),
                     pd.DataFrame(amt_usd_col)], axis=1)
                combined_df.columns = ['Whale Address', 'Label', 'Symbol', 'Amount', 'Amount USD']

                token_addr_data[token_name] = list(combined_df['Whale Address'].head(5).values)

            except:
                pass

        token_addr_data = {key.lower(): value for key, value in token_addr_data.items()}

        ##########################Linking in Arkham here




        ####base url = https://api.arkhamintelligence.com

        ####Breakdown by token

        ###will need to loop over x amt of timestamps and aggregate the data from there - can track changes in portfolio


        current_timestamp = datetime.datetime.now()
        timestamp_1d = current_timestamp - datetime.timedelta(days=1)
        timestamp_3d = current_timestamp - datetime.timedelta(days=3)
        timestamp_7d = current_timestamp - datetime.timedelta(days=7)
        timestamp_14d = current_timestamp - datetime.timedelta(days=14)
        timestamp_30d = current_timestamp - datetime.timedelta(days=30)

        current_timestamp_milliseconds = int(current_timestamp.timestamp() * 1000)
        timestamp_1d_milliseconds = int(timestamp_1d.timestamp() * 1000)
        timestamp_3d_milliseconds = int(timestamp_3d.timestamp() * 1000)
        timestamp_7d_milliseconds = int(timestamp_7d.timestamp() * 1000)
        timestamp_14d_milliseconds = int(timestamp_14d.timestamp() * 1000)
        timestamp_30d_milliseconds = int(timestamp_30d.timestamp() * 1000)

        timestamp_lists = []

        timestamp_lists.append(current_timestamp_milliseconds)
        timestamp_lists.append(timestamp_1d_milliseconds)
        timestamp_lists.append(timestamp_3d_milliseconds)
        timestamp_lists.append(timestamp_7d_milliseconds)
        timestamp_lists.append(timestamp_14d_milliseconds)
        timestamp_lists.append(timestamp_30d_milliseconds)

        ###1D


        hist_port_values = {}
        hist_port_values_chain = {}

        # token_addr_data  = {'white':['0x117f55bf3c2e3bcdc7f308504480ee53f754a7ca','0x2a58f09764479697a0f3f319f4e1774b18ff1edc'],'gem':['0x1d17d737fbcc091d38a64eff280dc6c6f6411a40','0x14b7d432dd102b50ecbd3005c93ce794a42f94a0']}


        combined_address_data_df = pd.DataFrame()

        for key, values in token_addr_data.items():

            relevant_token = key
            address_all = values

            for addr in (address_all):

                for t in timestamp_lists:

                    r = requests.get(f'https://api.arkhamintelligence.com/portfolio/address/{addr}?time={t}',
                                     headers={'API-Key': f'{arkham_api_key}'})
                    data = r.json()

                    if not data:
                        continue

                    list_of_chains = []

                    for key, values in data.items():
                        list_of_chains.append(key)

                    result = {}
                    for chain, tokens in data.items():
                        if tokens:  # Check if the tokens dictionary is not empty
                            token_data = []
                            for token_info in tokens.values():
                                symbol = token_info.get('symbol', '')
                                balance = token_info.get('balance', '')
                                usd = token_info.get('usd', '')
                                token_data.append([symbol, str(balance), str(usd)])
                            result[chain] = token_data

                    hist_port_values[t] = result

                    result_agg = {}

                    for chain, tokens in data.items():
                        if tokens:
                            total_balance = sum(token_info.get('balance', 0) for token_info in tokens.values())
                            total_usd = sum(token_info.get('usd', 0) for token_info in tokens.values())

                            result_agg[chain] = [total_balance, total_usd]

                    hist_port_values_chain[t] = result_agg

                try:
                    ####CURRENT

                    current_val = hist_port_values[timestamp_lists[0]]

                    current_val_final = {}
                    final_current_val_port = {}

                    for keys, values in current_val.items():
                        for l in range(len(values)):
                            final_current_val_port[values[l][0]] = [values[l][1], values[l][2]]

                    final_current_val_port = pd.DataFrame(final_current_val_port).T

                    try:
                        final_current_val_port.columns = ['Balance', 'USD']
                    except:
                        final_current_val_port = pd.DataFrame()

                    ###1d

                    one_day = hist_port_values[timestamp_lists[1]]

                    final_one_day_port = {}

                    for keys, values in one_day.items():
                        for l in range(len(values)):
                            final_one_day_port[values[l][0]] = [values[l][1], values[l][2]]

                    final_one_day_port = pd.DataFrame(final_one_day_port).T
                    try:
                        final_one_day_port.columns = ['Balance', 'USD']
                    except:
                        pass

                    ###3d

                    three_day = hist_port_values[timestamp_lists[2]]

                    final_three_day_port = {}

                    for keys, values in three_day.items():
                        for l in range(len(values)):
                            final_three_day_port[values[l][0]] = [values[l][1], values[l][2]]

                    final_three_day_port = pd.DataFrame(final_three_day_port).T
                    try:
                        final_three_day_port.columns = ['Balance', 'USD']
                    except:
                        pass

                    ###7d

                    seven_day = hist_port_values[timestamp_lists[3]]

                    final_seven_day_port = {}

                    for keys, values in seven_day.items():
                        for l in range(len(values)):
                            final_seven_day_port[values[l][0]] = [values[l][1], values[l][2]]

                    final_seven_day_port = pd.DataFrame(final_seven_day_port).T
                    try:
                        final_seven_day_port.columns = ['Balance', 'USD']
                    except:
                        pass

                    ##14d

                    fourteen_day = hist_port_values[timestamp_lists[4]]

                    final_fourteen_day_port = {}

                    for keys, values in fourteen_day.items():
                        for l in range(len(values)):
                            final_fourteen_day_port[values[l][0]] = [values[l][1], values[l][2]]

                    final_fourteen_day_port = pd.DataFrame(final_fourteen_day_port).T
                    try:
                        final_fourteen_day_port.columns = ['Balance', 'USD']
                    except:
                        pass

                    ##30d

                    thirty_day = hist_port_values[timestamp_lists[5]]
                    final_thirty_day_port = {}

                    for keys, values in thirty_day.items():
                        for l in range(len(values)):
                            final_thirty_day_port[values[l][0]] = [values[l][1], values[l][2]]

                    final_thirty_day_port = pd.DataFrame(final_thirty_day_port).T
                    try:
                        final_thirty_day_port.columns = ['Balance', 'USD']
                    except:
                        pass

                    final_current_val_port = final_current_val_port.astype(float)
                    final_one_day_port = final_one_day_port.astype(float)
                    final_three_day_port = final_three_day_port.astype(float)
                    final_seven_day_port = final_seven_day_port.astype(float)
                    final_fourteen_day_port = final_fourteen_day_port.astype(float)
                    final_thirty_day_port = final_thirty_day_port.astype(float)


                    #####track changes now and create a combined final hist port table

                    # Calculate percentage changes
                    percentage_changes_1d = ((final_current_val_port - final_one_day_port) / final_current_val_port) * 100
                    percentage_changes_1d.fillna(0, inplace=True)

                    percentage_changes_3d = ((final_current_val_port - final_three_day_port) / final_current_val_port) * 100
                    percentage_changes_3d.fillna(0, inplace=True)


                    percentage_changes_7d = ((final_current_val_port - final_seven_day_port) / final_current_val_port) * 100
                    percentage_changes_7d.fillna(0, inplace=True)

                    percentage_changes_14d = ((final_current_val_port - final_fourteen_day_port) / final_current_val_port) * 100
                    percentage_changes_14d.fillna(0, inplace=True)

                    percentage_changes_30d = ((final_current_val_port - final_thirty_day_port) / final_current_val_port) * 100
                    percentage_changes_30d.fillna(0, inplace=True)

                    try:
                        final_current_val_port['1D Chg'] = percentage_changes_1d['Balance']
                    except:
                        pass

                    try:
                        final_current_val_port['3D Chg'] = percentage_changes_3d['Balance']
                    except:
                        pass

                    try:
                        final_current_val_port['7D Chg'] = percentage_changes_7d['Balance']
                    except:
                        pass
                    try:
                        final_current_val_port['14D Chg'] = percentage_changes_14d['Balance']
                    except:
                        pass

                    try:
                        final_current_val_port['30D Chg'] = percentage_changes_30d['Balance']
                    except:
                        pass

                    final_current_val_port = final_current_val_port.sort_values(by='USD', ascending=False)


                    # Define a custom formatting function
                    def format_decimal(value):
                        if isinstance(value, float):
                            return '{:.2f}'.format(value)
                        return value


                    # Apply the custom formatting function to convert scientific notation to decimal notation
                    final_current_val_port = final_current_val_port.applymap(format_decimal)

                    ###only limit to the token in question

                    final_current_val_port = final_current_val_port[final_current_val_port.index == relevant_token]
                    combined_address_data_df = pd.concat([combined_address_data_df, final_current_val_port])




                except:
                    pass

        combined_address_data_df = combined_address_data_df.astype('float')

        combined_address_data_df = combined_address_data_df.reset_index()

        print(combined_address_data_df)

        # Use groupby and aggregation to combine rows
        combined_address_data_df = combined_address_data_df.groupby(['index']).agg(
            {'Balance': 'sum', 'USD': 'sum', '1D Chg': 'sum','3D Chg': 'sum','7D Chg': 'sum', '14D Chg': 'sum', '30D Chg': 'sum'}).reset_index()

        print(combined_address_data_df)

        return combined_sector_df, combined_address_data_df
