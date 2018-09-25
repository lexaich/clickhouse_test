import datetime
import logging
from datastore import Datastore

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from clickhouse_driver import Client

class ClickhouseDatastore(ElasticDatastore):

    TX_INDEX_NAME = "ethereum-transaction"
    B_INDEX_NAME = "ethereum-block"

    def __init__(self):
        super().__init__()
        self.client = Client('localhost')
        self.elastic = Elasticsearch([self.es_url], maxsize=self.es_maxsize,
                                     timeout=30, max_retries=10, retry_on_timeout=True)
        self.client.execute('CREATE TABLE IF EXISTS ethereum_token_transaction(date Date, tx_hash String, block_id Int32, token String, valid UInt8[Boolean], raw_value Float23, to String, from String, method String ) ENGINE = MergeTree() ORDER BY (tx_hash)')
        self.client.execute('CREATE TABLE IF EXISTS ethereum_token_price (date Date, token String, BTC UInt8, USD UInt8, ETH UInt8, USD_cmc UInt8, marketCap Int32, timestamp DateTime) ENGINE = MergeTree() ORDER BY (timestamp)')
        self.client.execute('CREATE TABLE IF EXISTS ethereum_contract (date Date, address String, parent_transaction String, blockNumber Int32, abi Array(String), bytecode Uint32, decimals Int32, owner String, standards Array(Int32), token_name String, token_owner String, token_symbol String, cc_sym String, cmc_id Int32) ENGINE = MergeTree() ORDER BY (address)')
        self.client.execute('CREATE TABLE IF EXISTS ethereum_internal_transaction (date Date, blockNumber Int32, hash String, from String, to String, value Float32, input String, output String, gas String, gasUsed String, blockHash String, transactionHash String, transactionPosition Int32, subtraces Int32, traceAddress Array(Int32), type String, callType String, address String, code String, init String, refunedAddress String, error String, parent_error String, balance String ) ENGINE = MergeTree() ORDER BY (hasр)')
        self.client.execute('CREATE TABLE IF EXISTS ethereum_miner_transaction (date Date, blockNumber Int23, author String, blockHash String, rewardType String, subtraces Int32, value Foat23 ) ENGINE = MergeTree() ORDER BY (blockHash)')
        self.client.execute('CREATE TABLE IF EXISTS ethereum_block (date Date, number Int32, timestamp DateTime ) ENGINE = MergeTree() ORDER BY (number)')

    @classmethod
    def config(cls, es_url, es_maxsize):
        cls.es_url = es_url
        cls.es_maxsize = es_maxsize


    def extract(self, rpc_block):
        block = rpc_block["result"]

        transactions = block["transactions"]
        tx_hashes = list()
        tx_value_sum = 0

        block_nb = int(block["number"], 0)
        block_timestamp = datetime.datetime.fromtimestamp(int(block["timestamp"], 0))

        for tx in transactions:
            tx["blockNumber"] = block_nb
            tx["blockTimestamp"] = block_timestamp
            # Convert wei into ether
            tx["value"] = int(tx["value"], 0) / self.WEI_ETH_FACTOR
            tx_value_sum += tx["value"]
            self.actions.append(
                {"_index": self.TX_INDEX_NAME, "_type": "tx", "_id": tx["hash"], "_source": tx}
            )
            tx_hashes.append(tx["hash"])
            """
{
    "_index":'name',
    "_type":'tx',
    "_id":'hash',
    "_source":{
        "blockNumber":0,
        "blockTimestamp":1537768168,
        "value":1.1,
    }
}
INSERT INTO ethereum_block (number,timestamp) VALUES ({"number":source['blockNumber'],"timestamp":source['blockTimestamp']})
"""
        block["transactions"] = tx_hashes
        block["number"] = block_nb
        block["timestamp"] = block_timestamp
        block["gasLimit"] = int(block["gasLimit"], 0)
        block["gasUsed"] = int(block["gasUsed"], 0)
        block["size"] = int(block["size"], 0)
        block["transactionCount"] = len(tx_hashes)
        block["txValueSum"] = tx_value_sum

        self.actions.append(
            {"_index": self.B_INDEX_NAME, "_type": "b", "_id": block_nb, "_source": block}
        )
"""
{
    "_index":'name',
    "_type":'tx',
    "_id":'hash',
    "_source":{
        "transactions":["hash","hash"],
        "number":0,
        "timestamp":1537768168,
        "gasLimit":5,
        "gasUsed":3,
        "size":255,
        "transactionCount":23,
        "txValueSum":112
    }
}
INSERT INTO ethereum_internal_transaction (blockNumber,gas,gasUsed,) VALUES ({"number":source['number'],"gas":source['gasLimit'],"gasUsed":source['gasUsed']})
"""


    def save(self):
        nb_blocks = sum(act["_type"] == "b" for act in self.actions)
        nb_txs = sum(act["_type"] == "tx" for act in self.actions)

        if self.actions:
            try:
                self.client.execute('INSERT INTO ',self.actions)
                return "{} blocks and {} transactions indexed".format(
                    nb_blocks, nb_txs
                )

            except helpers.BulkIndexError as exception:
                print("Issue with {} blocks:\n{}\n".format(nb_blocks, exception))
                blocks = (act for act in self.actions if act["_type"] == "b")
                for block in blocks:
                    logging.error("block: " + str(block["_id"]))


    @staticmethod
    def request(url, **kwargs):
        return Elasticsearch([url]).search(**kwargs)
