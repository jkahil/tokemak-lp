from config import PROVIDER_URL, UNI_FEES, SUSHI_FEES
from archive_node.node import *
from utils import *
import pandas as pd

FROM_BLOCK = 9000000  # put start of univ3

w3 = web3.Web3(web3.HTTPProvider(PROVIDER_URL))
LATEST_BLOCK = w3.eth.get_block_number()

# Load the mapping file that maps block numbers to dates
block_to_date = pd.read_csv('data/block_date_map.csv', index_col=0, sep='\t')


# create a class uniV2 wil be easier

class Lp2:
    def __init__(self, pool_address, universe):
        """
        Takes a pool_address and a universe dataframe with all pool info
        """
        pool_data = universe[universe['id'].str.startswith(pool_address)]
        self.token0 = pool_data.iloc[0]['token0.symbol']
        self.token1 = pool_data.iloc[0]['token1.symbol']
        self.pool_address = pool_address
        self.exchange = pool_data.iloc[0]['Exchange']
        self.token0_decimals = pool_data.iloc[0]['token0.decimals']
        self.token1_decimals = pool_data.iloc[0]['token1.decimals']
        self.pool_abi = get_ABI(self.pool_address)

    def get_fees(self):
        if self.exchange == 'UNI':
            trading_fee = UNI_FEES
        else:
            trading_fee = SUSHI_FEES
        swa = extractSwap(self.pool_address, pool_abi=self.pool_abi)
        if self.token0 == 'WETH':
            for c in ['amount0In', 'amount0Out']:
                swa[c] = swa[c] / 10 ** (self.token0_decimals)
            swa['ETH vol'] = swa['amount0Out'] + swa['amount0In']
        else:
            for c in ['amount1In', 'amount1Out']:
                swa[c] = swa[c] / 10 ** (self.token0_decimals)
            swa['ETH vol'] = swa['amount1Out'] + swa['amount1In']
        swa['ETH fees'] = swa['ETH vol'] * trading_fee
        # convert block number to actual date
        swa['day'] = swa['blockNumber'].apply(lambda x: blockToDate(x, block_to_date))
        daily_fees = pd.pivot_table(data=swa, index='day', values='ETH fees', aggfunc='sum')  # pivot_table
        daily_volumes = pd.pivot_table(data=swa, index='day', values='ETH vol', aggfunc='sum')
        return daily_fees.join(daily_volumes)

    def get_reserves(self):
        sync = extractSync(self.pool_address, pool_abi=self.pool_abi)
        sync['reserve0_adj'] = sync['reserve0'] / 10 ** (self.token0_decimals)
        sync['reserve1_adj'] = sync['reserve1'] / 10 ** (self.token1_decimals)
        if self.token0 == 'WETH':
            sync['Token vs WETH'] = sync['reserve1_adj'] / sync['reserve0_adj']
            sync['TVL ETH'] = 2 * sync['reserve0_adj']
        else:
            sync['Token vs WETH'] = sync['reserve0_adj'] / sync['reserve1_adj']
            sync['TVL ETH'] = 2 * sync['reserve1_adj']
        sync['day'] = sync['blockNumber'].apply(lambda x: blockToDate(x, block_to_date))
        sync = sync.set_index('day')
        self.reserves = sync
        self.blocklist = block_to_date.loc[sync.index.dropna().unique()]['Block'].unique()
        return sync

    # get daily Nb Lp tokens could be faster by archive node but could consume more calls
    def get_supply(self):
        sup = supply(self.pool_address, pool_abi=self.pool_abi, list_block=self.blocklist)
        sup.index = sup.index.map(lambda x: blockToDate(x, block_to_date))
        sup=sup[~sup.index.duplicated(keep='last')]
        return sup


# Running example
address = "0x21b8065d10f73ee2e260e5b47d3344d3ced7596e"
universe = pd.read_csv('data/research_universe.csv')
lp = Lp2(address, universe)
df_fee = lp.get_fees()
df_tvl = lp.get_reserves()
df_tvl=df_tvl[~df_tvl.index.duplicated(keep='last')]
df_supply = lp.get_supply()
# join doubles up check to remove duplicates
pool_data_summary = df_fee.join(df_supply).join(df_tvl)

pool_data_summary.to_csv('data/Node/' + str(address) + '_summary.csv')
print('pp')
