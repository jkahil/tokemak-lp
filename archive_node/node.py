import requests
import time
import pandas as pd
from config import PROVIDER_URL
import web3

w3 = web3.Web3(web3.HTTPProvider(PROVIDER_URL))
LATEST_BLOCK=w3.eth.get_block_number()

def get_ABI(contract_address):
  """
  get the contract ABi
  :param contract_address:
  :return:
  """
  res = requests.get("https://api.etherscan.io/api?module=contract&action=getabi&address="+contract_address)
  POOL_ABI = res.json()['result']
  if POOL_ABI=='Contract source code not verified':
    time.sleep(10)
    res = requests.get("https://api.etherscan.io/api?module=contract&action=getabi&address=0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
    POOL_ABI = res.json()['result']
    print(POOL_ABI)
    return(POOL_ABI)
  else:
    return (POOL_ABI)


def _getEvents(contract_address, event_name, start_block, end_block, pool_abi,node=w3):
  myContract = node.eth.contract(address=web3.Web3.toChecksumAddress(contract_address), abi=pool_abi)
  filter_builder = myContract.events[event_name].build_filter()
  filter_builder.fromBlock = start_block
  filter_builder.toBlock = end_block
  print('check:', type(start_block), type(end_block))
  ff = filter_builder.deploy(w3=node)
  try:
    df_data = ff.get_all_entries()
  except:
    raise ValueError("too many results")
  else:
    df = pd.DataFrame(df_data)
    return cleanLog(df)


def getEvents(contract_address, pool_abi,event_name='Mint', start_block=9000000, end_block=LATEST_BLOCK,node=w3):
  df = pd.DataFrame()
  # we need to check how many instances we need to retrieve per query as they are limited to 10000
  step = 100000
  download = True

  if end_block == "latest":
    eblock = LATEST_BLOCK
  else:
    eblock = min(end_block, LATEST_BLOCK)
  sblock = int(max(start_block, eblock - step))
  print(sblock, eblock)

  while download:
    print("download:", sblock, eblock, step)
    try:
      df1 = _getEvents(contract_address, event_name, sblock, eblock, pool_abi,node=node)
    except ValueError:
      print("wrong")
      step = int(step / 2)
      print(step)
      sblock = max(0, eblock - step)
      time.sleep(5)  # to avoid hitting API limit
    else:
      print("success", len(df1))
      if len(df1) > 0:
        df = df1.append(df)
        # update the step so we have 90% capacity
        step = int(step * 10000 / len(df1) * 0.9)
        eblock = sblock - 1
        sblock = max(start_block, max(0, eblock - step))
        time.sleep(5)

        if eblock < sblock:
          download = False
        else:
          download = True

      else:
        download = False

  return df

def extractSwap(contract_address, pool_abi,start_block=900000,node=w3):

  swap = getEvents(contract_address, pool_abi,'Swap', start_block, LATEST_BLOCK,node=node)
  print('swap positions extracted')
  return swap


def extractSync(contract_address, pool_abi,start_block=900000,node=w3):
  """
  Extracts uni v2 Reserves
  :param contract_address:
  :param start_block:
  :return:
  """
  reserves = getEvents(contract_address, pool_abi, event_name='Sync', start_block=start_block,end_block= "latest",node=w3)
  print('swap positions extracted')
  return reserves  # [TO_KEEP_SWAP]

def cleanLog(df_data):
  if len(df_data)>0:
    df = pd.DataFrame(df_data)
    for key in df['args'].iloc[0].keys():
        df[key] = df['args'].apply(lambda l: l[key])
    df = df.drop(['args'], axis=1)
    for column in ['address', 'blockHash', 'event', 'transactionHash']:
        df[column] = df[column].astype(str)
    return df
  else:
    return pd.DataFrame()

def _supply(contract_address,block,pool_abi,node=w3):
  """

  :param contract_address: contract address to query
  :param list_block: block we want to query
  :return:
  """
  myContract = node.eth.contract(address=web3.Web3.toChecksumAddress(contract_address), abi=pool_abi)
  try:
    total=myContract.functions.totalSupply().call(block_identifier=int(block))
  except:
    total=0
  return pd.DataFrame(total,index=[int(block)],columns=['Supply'])

def supply(contract_address,list_block,pool_abi):
  """

  :param contract_address: contract address to query
  :param list_block: array of block we want to query
  :return:
  """
  return pd.concat([_supply(contract_address,b,pool_abi) for b in list_block])