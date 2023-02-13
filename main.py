import pandas as pd
from gql import gql
import os
import requests
from math import floor, sqrt
import matplotlib.pyplot as plt
import utils
from utils import client, timestampToDate
from config import SUSHI_GRAPH_API, UNIV2_GRAPH_API, DAYS_PER_YEAR


class Pools:
    def __init__(self, url=UNIV2_GRAPH_API, min_tvl_USD=5000000, to_include=[], exchange='UNI'):
        """

        :param url: GraphQL url to use
        :param min_tvl_USD: Minimum investment threshold in USD \
        We only consider pools which have a certain amount for scalability reasons
        :param to_include: array of token symbols that we need to include  for example \
        only WETH pools
        """
        self.client = client(url)
        self.min_tvl = min_tvl_USD
        self.to_include = to_include
        self.exchange = exchange

        today = utils.todayTimestamp()
        self.timestamp = floor(today / 86400) * 86400  # last daily update timestamp

    def search(self):
        """
        Returns all the pools that matches our tvl and token criteria
        :return: pools addresses
        """
        filter_query = '''query($reserve_min: BigDecimal!,$date: Int!) {pairDayDatas(
            orderBy: date
            orderDirection: desc
            subgraphError: allow
            where: {reserveUSD_gt: $reserve_min, date:$date}
            first: 1000
          ) {
            reserveUSD
            token0 {
              id
              name
              symbol
              decimals
            }
            token1 {
              id
              name
              symbol
              decimals
            }
            date
            id
          }
        }'''
        params = {"reserve_min": self.min_tvl,
                  "date": self.timestamp}
        cli = self.client
        response = cli.execute(gql(filter_query), variable_values=params)
        data = pd.json_normalize(response['pairDayDatas'])
        data['exchange'] = self.exchange
        # fill in the token symbols
        if len(self.to_include) == 0:
            return data
        else:
            shortlist = data[
                (data['token1.symbol'].isin(self.to_include)) | (data['token0.symbol'].isin(self.to_include))]
            return shortlist


class LP:
    def __init__(self, exchange, pool_address, initial_stake, fees, start_ts):
        """
        :param pool_address: address of pool we are analysing
        :param exchange: 'UNI' for uniswap and 'SUSHI' for Sushiswap
        :param initial_stake: % of reserves owned at inception
        :param fees: trading fees charged by exchange that goes to LP
        :param start_ts: timestamp we start the analysis and deposit
        """
        self.pool_address = pool_address
        self.initial_stake = initial_stake
        self.fees = fees
        self.start_ts = start_ts
        self.exchange = exchange
        if exchange == 'UNI':
            self.graph_api = UNIV2_GRAPH_API
        else:
            if exchange == 'SUSHI':
                self.graph_api = SUSHI_GRAPH_API
        self.client = client(self.graph_api)

    def getHistUNI(self):
        """
        Retrieves the daily history of the pool fees, reserves
        :return:
        """
        hist_query = '''query($pairAddress:Bytes!,$date: Int!)
        {
            pairDayDatas(
                subgraphError: allow
            orderBy: date
            orderDirection: desc
            where: {pairAddress: $pairAddress, date_gte:$date}
            first: 800
        ) {
            totalSupply
        reserveUSD
        reserve1
        reserve0
        pairAddress
        id
        date
        dailyVolumeUSD
        dailyVolumeToken0
        dailyVolumeToken1
        token0
        {
            symbol
        decimals
        derivedETH
        }
        token1
        {
            derivedETH
        symbol
        name
        }
        }
        }'''
        params = {"pairAddress": self.pool_address,
                  "date": self.start_ts}
        cli = self.client
        response = cli.execute(gql(hist_query), variable_values=params)
        data = pd.json_normalize(response['pairDayDatas'])
        data = data.set_index('date')
        data = data.sort_index(ascending=True)
        self.token0 = data['token0.symbol'].iloc[0]
        self.token1 = data['token1.symbol'].iloc[0]
        """
        Convert columns to float
        """
        to_convert = ['totalSupply', 'dailyVolumeToken0', 'token0.derivedETH', 'reserve1', 'reserve0',
                      'dailyVolumeToken1']
        for col in to_convert:
            data[col] = data[col].astype('float32')
        if data.iloc[0]['token0.symbol'] == 'WETH':
            data['daily_fees_ETH'] = data['dailyVolumeToken0'] * self.fees
            data['daily_Volume_ETH'] = data['dailyVolumeToken0']
            data['reserve_ETH'] = data['reserve0'] * 2  # by definition of uniV2 you have 50/50 reserves
            data['WETH/Token'] = data['reserve1'] / data['reserve0']
        else:
            if data.iloc[1]['token1.symbol'] == 'WETH':
                data['daily_fees_ETH'] = data['dailyVolumeToken1'] * self.fees
                data['reserve_ETH'] = data['reserve1'] * 2
                data['daily_Volume_ETH'] = data['dailyVolumeToken1']
                data['WETH/Token'] = data['reserve0'] / data['reserve1']
            else:  # we do not have WETH in any leg
                data['daily_fees_ETH'] = data['dailyVolumeToken1'] * data['token0.derivedETH']
                data['WETH/Token'] = 0
        self.hist_data = data
        self.getRewards()
        return data

    def getHistSUSHI(self):
        """
        Retrieves the daily history of the pool fees, reserves
        :return:
        """
        hist_query = '''query($pairAddress:String!,$date: Int!)
        {
            pairDayDatas(
                subgraphError: allow
            orderBy: date
            orderDirection: desc
            where: {pair: $pairAddress, date_gte:$date}
            first: 800
        ) {
            totalSupply
        reserveUSD
        reserve1
        reserve0
        pair
        {
        id
        }
        date
        volumeUSD
        volumeToken0
        volumeToken1
        token0
        {
            symbol
        decimals
        derivedETH
        }
        token1
        {
            derivedETH
        symbol
        name
        }
        }
        }'''
        params = {"pairAddress": self.pool_address,
                  "date": self.start_ts}
        cli = self.client
        response = cli.execute(gql(hist_query), variable_values=params)
        data = pd.json_normalize(response['pairDayDatas'])
        data = data.set_index('date')
        data = data.sort_index(ascending=True)
        self.token0 = data['token0.symbol'].iloc[0]
        self.token1 = data['token1.symbol'].iloc[0]
        """
        Convert columns to float
        """
        to_convert = ['totalSupply', 'volumeToken0', 'token0.derivedETH', 'reserve1', 'reserve0', 'volumeToken1']
        for col in to_convert:
            data[col] = data[col].astype('float32')
        if data.iloc[0]['token0.symbol'] == 'WETH':
            data['daily_fees_ETH'] = data['volumeToken0'] * self.fees
            data['daily_Volume_ETH'] = data['volumeToken0']
            data['reserve_ETH'] = data['reserve0'] * 2  # by definition of uniV2 you have 50/50 reserves
            data['WETH/Token'] = data['reserve1'] / data['reserve0']
        else:
            if data.iloc[1]['token1.symbol'] == 'WETH':
                data['daily_fees_ETH'] = data['volumeToken1'] * self.fees
                data['reserve_ETH'] = data['reserve1'] * 2
                data['daily_Volume_ETH'] = data['volumeToken1']
                data['WETH/Token'] = data['reserve0'] / data['reserve1']
            else:  # we do not have WETH in any leg
                data['daily_fees_ETH'] = data['volumeToken1'] * data['token0.derivedETH']
                data['WETH/Token'] = 0
        self.hist_data = data[data['reserve_ETH'] != 0]
        self.getRewards()
        return data

    def getRewards(self):
        if self.exchange == 'UNI':
            self.rewards = 0
        else:
            reward = requests.get('https://www.sushi.com/earn/api/pool/eth:' + self.pool_address)
            # reward=pd.json_normalize(pd.json_normalize(reward.json()['pair'])['farm.incentives'][0])
            print('x')
            try:
                reward = pd.json_normalize(pd.json_normalize(reward.json()['pair'])['farm.incentives'][0])
                self.rewards = reward.iloc[0]['apr']
            except:
                self.rewards = 0

    def getStats(self):
        """
        This function analyses the historical data extracted previously to calculate
         -fee apy
         -total return
         -token volatility
         -volume
         -TVL
         -Pool fees (ETH)
         -fees volatility
         -Drawdown
        :return: df_hist : historical data day by day, summary: summary_stats
        """
        start_total_lp = float(self.hist_data.iloc[0]['totalSupply'])
        ini_nav = float(self.hist_data.iloc[0]['reserve_ETH']) * self.initial_stake
        lp_position = self.initial_stake * start_total_lp
        df_hist = self.hist_data.copy()
        # Calculate the fees generated by LP position
        df_hist['ownership_pct'] = lp_position / df_hist['totalSupply']
        df_hist['Fee_lp'] = df_hist['daily_fees_ETH'] * df_hist['ownership_pct']
        df_hist['NAV_ETH'] = df_hist['reserve_ETH'] * df_hist['ownership_pct']
        df_hist['Cumulative_Ret'] = df_hist['NAV_ETH'] / ini_nav - 1
        # how much fees represent as % of NAV
        df_hist['Fee_pct_NAV'] = df_hist['Fee_lp'] / ini_nav
        df_hist['Drawdown'] = df_hist['NAV_ETH'] / df_hist['NAV_ETH'].expanding().max() - 1
        df_hist['Token Ret'] = df_hist['WETH/Token'].shift(1) / df_hist[
            'WETH/Token'] - 1  # inverted as we use WETH as reverse not reliable

        # convert index timestamp to date
        df_hist.index = df_hist.index.map(timestampToDate)
        # Create a directory if does not exist
        folder_name = "data/" + self.token0 + '_' + self.token1 + '_' + self.exchange
        if not os.path.isdir(folder_name):
            # if the demo_folder2 directory is
            # not present then create it.
            os.makedirs(folder_name)
        # Charts to show NAV through time, fee component, TVL
        plt.clf()
        (df_hist['Cumulative_Ret'] * 100).plot(title=self.token0 + '_' + self.token1 + '_' + self.exchange,
                                               figsize=(12, 9))

        ((((1 + df_hist.iloc[1:]['Fee_pct_NAV']).cumprod()) - 1) * 100).plot(label='Cumulative Fee Return')
        plt.xlabel('timestamp')
        plt.ylabel('%')
        plt.legend()
        plt.savefig(folder_name + '/daily returns.png')
        plt.clf()
        ax = df_hist['reserve_ETH'].plot(title=self.token0 + '_' + self.token1 + '_' + self.exchange, label='TVL',
                                         figsize=(12, 9))
        ax2 = ax.twinx()
        df_hist['daily_Volume_ETH'].plot(label='Daily volume', color='r', ax=ax2)
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        print(labels + labels2)
        ax2.legend(lines + lines2, labels + labels2, loc=0)

        plt.xlabel('Timestamp')
        plt.ylabel('ETH')
        plt.savefig(folder_name + '/volume_tvl.png')

        # Generate the summary dataframe
        summary = pd.DataFrame(index=[self.pool_address],
                               columns=['nbDays', 'TVL_ETH', 'CumulRet', 'FeesRet', 'AnnRet', 'AnnVol', \
                                        'MaxDrawdown', 'Fees/Vol'])
        summary['nbDays'] = len(df_hist) - 1  # remove one day as we start at the end of day 1
        summary['TVL_ETH'] = df_hist.iloc[-1]['reserve_ETH']
        summary['CumulRet'] = df_hist.iloc[-1]['Cumulative_Ret']
        summary['FeesRet'] = ((1 + df_hist.iloc[1:]['Fee_pct_NAV']).cumprod()).iloc[
                                 -1] - 1  # compounded we skip first instance
        summary['FeesAnn'] = (1 + summary['FeesRet']) ** (DAYS_PER_YEAR / summary['nbDays']) - 1
        summary['AnnRet'] = (1 + summary['CumulRet']) ** (
                    DAYS_PER_YEAR / summary['nbDays']) - 1  # check if we can log it
        summary['AnnVol'] = df_hist['Token Ret'].std() * sqrt(DAYS_PER_YEAR)  # daily_ret annualized
        summary['MaxDrawdown'] = df_hist['Drawdown'].min()
        summary['Fees/Vol'] = summary['FeesRet'] / summary['AnnVol']
        summary['Fees_30D_pct'] = ((1 + df_hist.iloc[-30:]['Fee_pct_NAV']).cumprod()).iloc[-1] - 1
        summary['Fees/Vol 30D'] = summary['Fees_30D_pct'] * 12 / summary['AnnVol']
        summary['Pair'] = self.token0 + '_' + self.token1
        summary['Extra Incentives APR'] = self.rewards
        return df_hist, summary

# Search the investable Pools
uni = Pools(to_include=['WETH'])
pool_uni = uni.search()
sushi = Pools(url=SUSHI_GRAPH_API, to_include=['WETH'], exchange='SUSHI')
pool_sushi = sushi.search()
# save it
pd.concat([pool_uni, pool_sushi]).to_csv('data/research_universe.csv')

final_data = pd.DataFrame()
# get the relevant info for Uniswap pool
for pool_id in pool_uni['id'].unique():
    print(pool_id)
    lp_wise = LP(exchange='UNI', pool_address=pool_id.split('-')[0], initial_stake=0.01, fees=0.003,
                 start_ts=1640991600)
    lp_wise_hist = lp_wise.getHistUNI()
    ret, s = lp_wise.getStats()
    final_data = final_data.append(s)

# get the relevant info for Sushiswap pool
for pool_id in pool_sushi['id'].unique():
    lp_wise = LP(exchange='SUSHI', pool_address=pool_id.split('-')[0], initial_stake=0.01, fees=0.003,
                 start_ts=1640991600)
    lp_wise_hist = lp_wise.getHistSUSHI()
    ret, s = lp_wise.getStats()
    final_data = final_data.append(s)

final_data.to_csv('final_lp_stats.csv')

