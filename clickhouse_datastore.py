import datetime
import logging
from datastore import Datastore
import pdb
 
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from clickhouse_driver import Client
from elasticdatastore import ElasticDatastore

class ClickhouseDatastore(ElasticDatastore):
    def __init__(self):
        super().__init__()
        self.client = Client('localhost')
        self.schema_block = {
            "number": "Int32",
            "timestamp": "DateTime",
            "gasLimit": "Int32",
            "gasUsed": "Int32"
        }
        self.schema_tx = {
            "blockNumber": "Int32",
            "from": "String",
            "to": "String",
            "hash": "String"
        }
        schema_block_string = ", ".join(["{} {}".format(a, b) for a, b in self.schema_block.items()])
        schema_tx_string = ", ".join(["{} {}".format(a, b) for a, b in self.schema_tx.items()])
        self.client.execute('CREATE TABLE IF NOT EXISTS ethereum_block ({}) ENGINE = MergeTree() ORDER BY (number)'.format(schema_block_string))
        self.client.execute('CREATE TABLE IF NOT EXISTS ethereum_transaction ({}) ENGINE = MergeTree() ORDER BY (hash)'.format(schema_tx_string))

    def save(self):
        nb_blocks = sum(act["_type"] == "b" for act in self.actions)
        nb_txs = sum(act["_type"] == "tx" for act in self.actions)

        if self.actions:
            try:
                out_b = []
                out_tx = []
                for item in self.actions:
                    if item['_type'] == 'b':
                        out_b.append(item['_source'])
                    if (item['_type'] == 'tx') and (item["_source"][""]):
                        out_tx.append(item['_source'])
                
                schema_block_string = ",".join(self.schema_block.keys())
                schema_tx_string = ",".join(self.schema_tx.keys())
                self.client.execute('INSERT INTO ethereum_transaction ({}) VALUES'.format(schema_tx_string), out_tx)
                self.client.execute('INSERT INTO ethereum_block ({}) VALUES'.format(schema_block_string), out_b)

                return "{} blocks and {} transactions indexed".format(
                    nb_blocks, nb_txs
                )

            except helpers.BulkIndexError as exception:
                print("Issue with {} blocks:\n{}\n".format(nb_blocks, exception))
                blocks = (act for act in self.actions if act["_type"] == "b")
                for block in blocks:
                    logging.error("block: " + str(block["_id"]))

