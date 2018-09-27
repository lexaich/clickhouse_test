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

        self.client.execute('CREATE TABLE IF NOT EXISTS ethereum_transaction (date Date, transactions Array(String), number Int32, timestamp DateTime,gasLimit Int32, gasUsed Int32, size Int32, transactionCount Int32, txValueSum Int32 ) ENGINE = MergeTree() ORDER BY (date)')

        self.client.execute('CREATE TABLE IF NOT EXISTS ethereum_block (date Date, number Int32, timestamp DateTime, to String, from String, hash String ) ENGINE = MergeTree() ORDER BY (date,hash)')


    def save(self):
        nb_blocks = sum(act["_type"] == "b" for act in self.actions)
        nb_txs = sum(act["_type"] == "tx" for act in self.actions)

        if self.actions:
            try:
                out_b = []
                out_tx = []
                for item in self.action:
                    if item['_type'] == 'b':
                        out_b.append(item['_source'])
                    if item['_type'] == 'tx':
                        out_tx.append(item['_source'])
                    
                self.client.execute('INSERT INTO ethereum_internal_transaction (transactions,number,timestamp,gasLimit,gasUsed,size,transactionCount,txValueSum) VALUES', out_tx )

                self.client.execute('INSERT INTO ethereum_block (number,timestamp,to,from,hash) VALUES', out_b)

                return "{} blocks and {} transactions indexed".format(
                    nb_blocks, nb_txs
                )

            except helpers.BulkIndexError as exception:
                print("Issue with {} blocks:\n{}\n".format(nb_blocks, exception))
                blocks = (act for act in self.actions if act["_type"] == "b")
                for block in blocks:
                    logging.error("block: " + str(block["_id"]))

