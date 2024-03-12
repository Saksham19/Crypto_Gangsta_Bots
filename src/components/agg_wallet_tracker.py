import requests
import datetime
import pandas as pd
from tqdm import tqdm
import os
from dotenv import load_dotenv


load_dotenv()
arkham_api_key = os.getenv('ARKHAM_API_KEY')


def agg_wallet_bot():




    fund_add = pd.read_excel('Wallet_List.xlsx', sheet_name='FUND')
    fund_add = fund_add[['Address 1', 'Address 2', 'Address 3', 'Address 4', 'Address 5']]

    walletAddress_lists = fund_add[['Address 1', 'Address 2', 'Address 3', 'Address 4', 'Address 5']].values.tolist()

    walletAddress = []

    for sublist in walletAddress_lists:
        for item in sublist:
            if isinstance(item, str):
                walletAddress.append(item)

    api_key = arkham_api_key

    ####base url = https://api.arkhamintelligence.com


    ####Breakdown by token

    ###will need to loop over x amt of timestamps and aggregate the data from there - can track changes in portfolio



    current_timestamp = datetime.datetime.now()
    timestamp_1h = current_timestamp - datetime.timedelta(hours=1)
    timestamp_6h = current_timestamp - datetime.timedelta(hours=6)
    timestamp_1d = current_timestamp - datetime.timedelta(days=1)
    timestamp_7d = current_timestamp - datetime.timedelta(days=7)
    timestamp_14d = current_timestamp - datetime.timedelta(days=14)
    timestamp_30d = current_timestamp - datetime.timedelta(days=30)


    current_timestamp_milliseconds = int(current_timestamp.timestamp() * 1000)
    timestamp_1h_milliseconds = int(timestamp_1h.timestamp()*1000)
    timestamp_6h_milliseconds = int(timestamp_6h.timestamp()*1000)
    timestamp_1d_milliseconds = int(timestamp_1d.timestamp() * 1000)
    timestamp_7d_milliseconds = int(timestamp_7d.timestamp() * 1000)
    timestamp_14d_milliseconds = int(timestamp_14d.timestamp() * 1000)
    timestamp_30d_milliseconds = int(timestamp_30d.timestamp() * 1000)

    timestamp_lists = []

    timestamp_lists.append(current_timestamp_milliseconds)
    timestamp_lists.append(timestamp_1h_milliseconds)
    timestamp_lists.append(timestamp_6h_milliseconds)
    timestamp_lists.append(timestamp_1d_milliseconds)
    timestamp_lists.append(timestamp_7d_milliseconds)
    timestamp_lists.append(timestamp_14d_milliseconds)
    timestamp_lists.append(timestamp_30d_milliseconds)

    print(timestamp_lists)


    ###1D

    combined_hist_port_current_all = pd.DataFrame()
    combined_hist_port_1h_all = pd.DataFrame()
    combined_hist_port_6h_all = pd.DataFrame()
    combined_hist_port_1d_all = pd.DataFrame()
    combined_hist_port_7d_all = pd.DataFrame()
    combined_hist_port_14d_all = pd.DataFrame()
    combined_hist_port_30d_all = pd.DataFrame()


    combined_hist_port_current_chain_all = pd.DataFrame()
    combined_hist_port_1h_chain_all = pd.DataFrame()
    combined_hist_port_6h_chain_all = pd.DataFrame()
    combined_hist_port_1d_chain_all = pd.DataFrame()
    combined_hist_port_7d_chain_all = pd.DataFrame()
    combined_hist_port_14d_chain_all = pd.DataFrame()
    combined_hist_port_30d_chain_all = pd.DataFrame()
    combined_hist_port_usd_chain_all = pd.DataFrame()

    for w in tqdm(walletAddress):

        hist_port_values = {}
        hist_port_values_chain = {}

        try:


            for t in timestamp_lists:

                r = requests.get(f'https://api.arkhamintelligence.com/portfolio/address/{w}?time={t}',
                                 headers={'API-Key': f'{api_key}'})
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

            ####current

            current_day = hist_port_values[timestamp_lists[0]]

            current_day_final = {}
            final_current_day_port = {}

            for keys, values in current_day.items():
                for l in range(len(values)):
                    final_current_day_port[values[l][0]] = [values[l][1], values[l][2]]

            final_current_day_port = pd.DataFrame(final_current_day_port).T
            final_current_day_port.columns = ['Balance', 'USD']

            combined_hist_port_current_all = pd.concat([combined_hist_port_current_all, final_current_day_port])



            ##1h

            one_hour = hist_port_values[timestamp_lists[1]]

            one_hour_final = {}
            final_one_hour_port = {}

            for keys, values in one_hour.items():
                for l in range(len(values)):
                    final_one_hour_port[values[l][0]] = [values[l][1], values[l][2]]

            final_one_hour_port = pd.DataFrame(final_one_hour_port).T
            final_one_hour_port.columns = ['Balance', 'USD']

            combined_hist_port_1h_all = pd.concat([combined_hist_port_1h_all, final_one_hour_port])



            ##6h


            six_hour = hist_port_values[timestamp_lists[2]]

            six_hour_final = {}
            final_six_hour_port = {}

            for keys, values in six_hour.items():
                for l in range(len(values)):
                    final_six_hour_port[values[l][0]] = [values[l][1], values[l][2]]

            final_six_hour_port = pd.DataFrame(final_six_hour_port).T
            final_six_hour_port.columns = ['Balance', 'USD']

            combined_hist_port_6h_all = pd.concat([combined_hist_port_6h_all, final_six_hour_port])




            ##1d


            one_day = hist_port_values[timestamp_lists[3]]

            one_day_final = {}
            final_one_day_port = {}

            for keys, values in one_day.items():
                for l in range(len(values)):
                    final_one_day_port[values[l][0]] = [values[l][1], values[l][2]]

            final_one_day_port = pd.DataFrame(final_one_day_port).T
            final_one_day_port.columns = ['Balance', 'USD']

            combined_hist_port_1d_all = pd.concat([combined_hist_port_1d_all, final_one_day_port])


            ###7d

            seven_day = hist_port_values[timestamp_lists[4]]

            final_seven_day_port = {}

            for keys, values in seven_day.items():
                for l in range(len(values)):
                    final_seven_day_port[values[l][0]] = [values[l][1], values[l][2]]

            final_seven_day_port = pd.DataFrame(final_seven_day_port).T
            final_seven_day_port.columns = ['Balance', 'USD']

            combined_hist_port_7d_all = pd.concat([combined_hist_port_7d_all, final_seven_day_port])

            ##14d

            fourteen_day = hist_port_values[timestamp_lists[5]]

            final_fourteen_day_port = {}

            for keys, values in fourteen_day.items():
                for l in range(len(values)):
                    final_fourteen_day_port[values[l][0]] = [values[l][1], values[l][2]]

            final_fourteen_day_port = pd.DataFrame(final_fourteen_day_port).T
            final_fourteen_day_port.columns = ['Balance', 'USD']

            combined_hist_port_14d_all = pd.concat([combined_hist_port_14d_all, final_fourteen_day_port])

            ##30d

            thirty_day = hist_port_values[timestamp_lists[6]]

            final_thirty_day_port = {}

            for keys, values in thirty_day.items():
                for l in range(len(values)):
                    final_thirty_day_port[values[l][0]] = [values[l][1], values[l][2]]

            final_thirty_day_port = pd.DataFrame(final_thirty_day_port).T
            final_thirty_day_port.columns = ['Balance', 'USD']



            final_current_day_port = final_current_day_port.astype(float)
            final_one_hour_port = final_one_hour_port.astype(float)
            final_six_hour_port = final_six_hour_port.astype(float)
            final_one_day_port = final_one_day_port.astype(float)
            final_seven_day_port = final_seven_day_port.astype(float)
            final_fourteen_day_port = final_fourteen_day_port.astype(float)
            final_thirty_day_port = final_thirty_day_port.astype(float)

            combined_hist_port_30d_all = pd.concat([combined_hist_port_30d_all, final_thirty_day_port])



            #####track changes now and create a combined final hist port table


            # Calculate percentage changes

            percentage_changes_1h = ((final_one_hour_port - final_current_day_port)/final_current_day_port)*100
            percentage_changes_1h.fillna(0,inplace=True)

            percentage_changes_6h = ((final_six_hour_port - final_current_day_port)/final_current_day_port)*100
            percentage_changes_6h.fillna(0,inplace=True)

            percentage_changes_1d = ((final_one_day_port - final_current_day_port)/final_current_day_port)*100
            percentage_changes_1d.fillna(0,inplace=True)


            percentage_changes_7d = ((final_seven_day_port - final_current_day_port) / final_current_day_port) * 100
            percentage_changes_7d.fillna(0, inplace=True)

            percentage_changes_14d = ((final_fourteen_day_port - final_current_day_port) / final_current_day_port) * 100
            percentage_changes_14d.fillna(0, inplace=True)

            percentage_changes_30d = ((final_thirty_day_port - final_current_day_port) / final_current_day_port) * 100
            percentage_changes_30d.fillna(0, inplace=True)


            final_one_day_port['1h Chg'] = percentage_changes_1h['Balance']
            final_one_day_port['6h Chg'] = percentage_changes_6h['Balance']
            final_one_day_port['1D Chg'] = percentage_changes_1d['Balance']
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
            final_current_day_port = final_current_day_port.applymap(format_decimal)

        except:
            pass


    ###aggregate the same ones together

    combined_hist_port_current_all = combined_hist_port_current_all.astype(float)
    combined_hist_port_1h_all = combined_hist_port_1h_all.astype(float)
    combined_hist_port_6h_all = combined_hist_port_6h_all.astype(float)
    combined_hist_port_1d_all = combined_hist_port_1d_all.astype(float)
    combined_hist_port_7d_all = combined_hist_port_7d_all.astype(float)
    combined_hist_port_14d_all = combined_hist_port_14d_all.astype(float)
    combined_hist_port_30d_all = combined_hist_port_30d_all.astype(float)




    combined_hist_port_current_all = combined_hist_port_current_all.groupby(combined_hist_port_current_all.index).sum()

    combined_hist_port_1h_all = combined_hist_port_1h_all.groupby(combined_hist_port_1h_all.index).sum()
    combined_hist_port_6h_all = combined_hist_port_6h_all.groupby(combined_hist_port_6h_all.index).sum()
    combined_hist_port_1d_all = combined_hist_port_1d_all.groupby(combined_hist_port_1d_all.index).sum()
    combined_hist_port_7d_all = combined_hist_port_7d_all.groupby(combined_hist_port_7d_all.index).sum()
    combined_hist_port_14d_all = combined_hist_port_14d_all.groupby(combined_hist_port_14d_all.index).sum()
    combined_hist_port_30d_all = combined_hist_port_30d_all.groupby(combined_hist_port_30d_all.index).sum()



    ####get the differences across 1/7/14/30d  - final portfolio output

    percentage_changes_1h = ((combined_hist_port_1h_all - combined_hist_port_current_all) / combined_hist_port_current_all) * 100
    percentage_changes_6h = ((combined_hist_port_6h_all - combined_hist_port_current_all) / combined_hist_port_current_all) * 100
    percentage_changes_1d = ((combined_hist_port_1d_all - combined_hist_port_current_all) / combined_hist_port_current_all) * 100
    percentage_changes_7d = ((combined_hist_port_7d_all - combined_hist_port_current_all) / combined_hist_port_1d_all) * 100
    percentage_changes_14d = ((combined_hist_port_14d_all - combined_hist_port_current_all) / combined_hist_port_current_all) * 100
    percentage_changes_30d = ((combined_hist_port_30d_all - combined_hist_port_current_all) / combined_hist_port_current_all) * 100


    combined_hist_port_1d_all['1h Chg'] = percentage_changes_1h['Balance']
    combined_hist_port_1d_all['6h Chg'] = percentage_changes_6h['Balance']
    combined_hist_port_1d_all['1D Chg'] = percentage_changes_1d['Balance']
    combined_hist_port_1d_all['7D Chg'] = percentage_changes_7d['Balance']
    combined_hist_port_1d_all['14D Chg'] = percentage_changes_14d['Balance']
    combined_hist_port_1d_all['30D Chg'] = percentage_changes_30d['Balance']

    combined_hist_port_1d_all = combined_hist_port_1d_all.sort_values(by='USD', ascending=False)


    combined_hist_port_1d_all = combined_hist_port_1d_all.dropna()

    combined_hist_port_1d_all = combined_hist_port_1d_all.head(20)


    return combined_hist_port_1d_all




















