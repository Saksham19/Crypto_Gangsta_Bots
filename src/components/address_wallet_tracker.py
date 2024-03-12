import requests
import datetime
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()
arkham_api_key = os.getenv('ARKHAM_API_KEY')


def wallet_bot(message_content):


    api_key = arkham_api_key

    ####base url = https://api.arkhamintelligence.com

    ####Breakdown by token

    ###will need to loop over x amt of timestamps and aggregate the data from there - can track changes in portfolio


    current_timestamp = datetime.datetime.now()
    timestamp_7d = current_timestamp - datetime.timedelta(days=7)
    timestamp_14d = current_timestamp - datetime.timedelta(days=14)
    timestamp_30d = current_timestamp - datetime.timedelta(days=30)

    current_timestamp_milliseconds = int(current_timestamp.timestamp() * 1000)
    timestamp_7d_milliseconds = int(timestamp_7d.timestamp() * 1000)
    timestamp_14d_milliseconds = int(timestamp_14d.timestamp() * 1000)
    timestamp_30d_milliseconds = int(timestamp_30d.timestamp() * 1000)

    timestamp_lists = []

    timestamp_lists.append(current_timestamp_milliseconds)
    timestamp_lists.append(timestamp_7d_milliseconds)
    timestamp_lists.append(timestamp_14d_milliseconds)
    timestamp_lists.append(timestamp_30d_milliseconds)

    print(timestamp_lists)

    ###1D


    hist_port_values = {}
    hist_port_values_chain = {}

    address = message_content

    for t in timestamp_lists:


        if address[0].isdigit():
            r = requests.get(f'https://api.arkhamintelligence.com/portfolio/address/{address}?time={t}',
                             headers={'API-Key': f'{api_key}'})
            data = r.json()
        else:
            r =requests.get(f'https://api.arkhamintelligence.com/portfolio/entity/{address}?time={t}',headers={'API-Key':f'{api_key}'})
            data = r.json()



        list_of_chains = []

        for key, values in data.items():
            list_of_chains.append(key)

        # print(data)

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

    print(hist_port_values)

    ####1d

    one_day = hist_port_values[timestamp_lists[0]]

    one_day_final = {}
    final_one_day_port = {}

    for keys, values in one_day.items():
        for l in range(len(values)):
            final_one_day_port[values[l][0]] = [values[l][1], values[l][2]]

    final_one_day_port = pd.DataFrame(final_one_day_port).T

    try:
        final_one_day_port.columns = ['Balance', 'USD']
    except:
        final_one_day_port = pd.DataFrame()

    ###7d

    seven_day = hist_port_values[timestamp_lists[1]]

    final_seven_day_port = {}

    for keys, values in seven_day.items():
        for l in range(len(values)):
            final_seven_day_port[values[l][0]] = [values[l][1], values[l][2]]

    final_seven_day_port = pd.DataFrame(final_seven_day_port).T
    final_seven_day_port.columns = ['Balance', 'USD']

    ##14d


    fourteen_day = hist_port_values[timestamp_lists[2]]

    final_fourteen_day_port = {}

    for keys, values in fourteen_day.items():
        for l in range(len(values)):
            final_fourteen_day_port[values[l][0]] = [values[l][1], values[l][2]]

    final_fourteen_day_port = pd.DataFrame(final_fourteen_day_port).T
    final_fourteen_day_port.columns = ['Balance', 'USD']

    ##30d

    thirty_day = hist_port_values[timestamp_lists[3]]
    final_thirty_day_port = {}

    for keys, values in thirty_day.items():
        for l in range(len(values)):
            final_thirty_day_port[values[l][0]] = [values[l][1], values[l][2]]

    final_thirty_day_port = pd.DataFrame(final_thirty_day_port).T
    try:
        final_thirty_day_port.columns = ['Balance', 'USD']
    except:
        pass

    final_one_day_port = final_one_day_port.astype(float)
    final_seven_day_port = final_seven_day_port.astype(float)
    final_fourteen_day_port = final_fourteen_day_port.astype(float)
    final_thirty_day_port = final_thirty_day_port.astype(float)

    #####track changes now and create a combined final hist port table

    # Calculate percentage changes
    percentage_changes_7d = ((final_one_day_port - final_seven_day_port) / final_one_day_port) * 100
    percentage_changes_7d.fillna(0, inplace=True)

    percentage_changes_14d = ((final_one_day_port - final_fourteen_day_port) / final_one_day_port) * 100
    percentage_changes_14d.fillna(0, inplace=True)

    percentage_changes_30d = ((final_one_day_port - final_thirty_day_port) / final_one_day_port) * 100
    percentage_changes_30d.fillna(0, inplace=True)

    final_one_day_port['7D Chg'] = percentage_changes_7d['Balance']
    final_one_day_port['14D Chg'] = percentage_changes_14d['Balance']
    final_one_day_port['30D Chg'] = percentage_changes_30d['Balance']

    final_one_day_port = final_one_day_port.sort_values(by='USD', ascending=False)


    # Define a custom formatting function
    def format_decimal(value):
        if isinstance(value, float):
            return '{:.2f}'.format(value)
        return value


    # Apply the custom formatting function to convert scientific notation to decimal notation
    final_one_day_port = final_one_day_port.applymap(format_decimal)
    final_one_day_port = final_one_day_port.head(15)
    print(final_one_day_port.head(20))

    #####portfolio - by chain


    hist_port_values_chain = pd.DataFrame.from_dict(hist_port_values_chain, orient='index')


    def get_first_element(lst):
        return lst[0] if isinstance(lst, list) and len(lst) > 0 else None


    def get_second_element(lst):
        return lst[1] if isinstance(lst, list) and len(lst) > 0 else None


    # Apply the custom function to each cell in the DataFrame
    balance_port = hist_port_values_chain.applymap(get_first_element)
    usd_port = hist_port_values_chain.applymap(get_second_element)

    try:
        seven_day_value_chain = ((balance_port.iloc[3] - balance_port.iloc[2]) / balance_port.iloc[3]) * 100
    except:
        seven_day_value_chain = 'N/A'

    try:
        fourteen_day_value_chain = ((balance_port.iloc[3] - balance_port.iloc[1]) / balance_port.iloc[3]) * 100
    except:
        fourteen_day_value_chain = 'N/A'

    try:
        thirty_day_value_chain = ((balance_port.iloc[3] - balance_port.iloc[0]) / balance_port.iloc[3]) * 100
    except:
        thirty_day_value_chain = 'N/A'

    current_value_chain = usd_port.iloc[3]

    try:
        hist_port_values_chain = pd.concat(
            [current_value_chain, seven_day_value_chain, fourteen_day_value_chain, thirty_day_value_chain], axis=1)

    except:
        try:
            hist_port_values_chain = pd.concat([current_value_chain, seven_day_value_chain, fourteen_day_value_chain],
                                               axis=1)
        except:
            try:
                hist_port_values_chain = pd.concat([current_value_chain, seven_day_value_chain], axis=1)
            except:
                hist_port_values_chain = pd.concat([current_value_chain], axis=1)

    # hist_port_values_chain = pd.concat([current_value_chain,seven_day_value_chain,fourteen_day_value_chain,thirty_day_value_chain],axis=1)

    try:
        hist_port_values_chain.columns = ['Agg. Value (USD)', '7D Chg', '14D Chg', '30D Chg']
    except:
        try:
            hist_port_values_chain.columns = ['Agg. Value (USD)', '7D Chg', '14D Chg']
        except:
            try:
                hist_port_values_chain.columns = ['Agg. Value (USD)', '7D Chg']
            except:
                hist_port_values_chain.columns = ['Agg. Value (USD)']

    hist_port_values_chain = hist_port_values_chain.sort_values(by='Agg. Value (USD)', ascending=False)

    hist_port_values_chain = hist_port_values_chain.applymap(format_decimal)

    print(hist_port_values_chain)

    return final_one_day_port,hist_port_values_chain















