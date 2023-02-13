from gql import gql, Client
from datetime import datetime
from math import sqrt
from gql.transport.requests import RequestsHTTPTransport
import time

TIMESTAMP_PER_YEAR=86400*360

def todayTimestamp():
    ts = int(time.time())
    return (ts)

def timestampToDate(ts):
    """

    :param ts: unix time stamp
    :return: date
    """
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

def blockToDate(block,map_table):
    """

    :param block: block number
    :return:
    """
    df_map=map_table[map_table['Block']<block]
    return df_map.index.max()



def client(api_url):
    """
    Initializes a connection to subgraph
    :param api_url: subgraph url for connection
    is_secure: do we need to use https
    :return:
    """
    sample_transport = RequestsHTTPTransport(
        url=api_url,
        headers= {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'},
        verify=True,
        retries=10,
    )
    client = Client(
        transport=sample_transport,
    fetch_schema_from_transport = True
    )
    return client

def portRet(token_perf):
    """
    :param token_perf:     Token Perf versus WETH or base
    :return: portfolio returns
    """
    port50_50=0.5+0.5*token_perf
    il= 2 * sqrt(2 + token_perf) /(2 + token_perf) -1
    lp_return=port50_50*(1+il)
    return lp_return




