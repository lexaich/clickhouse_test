from clickhouse_driver import Client

client = Client('localhost') #коннект к базе данных по умолчанию

#print(client.execute('SHOW TABLES'))

client.execute('DROP TABLE IF EXISTS ethereum-token-transaction')
# id#tx_hash
client.execute('CREATE TABLE ethereum_token_transaction(date Date, tx_hash String, block_id Int32, token String, valid UInt8[Boolean], raw_value Float23, to String, from String, method String ) ENGINE = MergeTree() ORDER BY (tx_hash)')

client.execute('DROP TABLE IF EXISTS ethereum_token_price')
# id#token_symbol+_+date
client.execute('CREATE TABLE ethereum_token_price (date Date, token String, BTC UInt8, USD UInt8, ETH UInt8, USD_cmc UInt8, marketCap Int32, timestamp DateTime) ENGINE = MergeTree() ORDER BY (timestamp)')

client.execute('DROP TABLE IF EXISTS ethereum_contract')
# id#address
client.execute('CREATE TABLE ethereum_contract (date Date, address String, parent_transaction String, blockNumber Int32, abi Array(String), bytecode Uint32, decimals Int32, owner String, standards Array(Int32), token_name String, token_owner String, token_symbol String, cc_sym String, cmc_id Int32) ENGINE = MergeTree() ORDER BY (address)')

client.execute('DROP TABLE IF EXISTS ethereum_internal_transaction')
# id#hash+_+position in trace
client.execute('CREATE TABLE ethereum_internal_transaction (date Date, blockNumber Int32, hash String, from String, to String, value Float32, input String, output String, gas String, gasUsed String, blockHash String, transactionHash String, transactionPosition Int32, subtraces Int32, traceAddress Array(Int32), type String, callType String, address String, code String, init String, refunedAddress String, error String, parent_error String, balance String ) ENGINE = MergeTree() ORDER BY (hasр)')



client.execute('DROP TABLE IF EXISTS ethereum_miner_transaction')
 # id#blockHash
client.execute('CREATE TABLE ethereum_miner_transaction (date Date, blockNumber Int23, author String, blockHash String, rewardType String, subtraces Int32, value Foat23 ) ENGINE = MergeTree() ORDER BY (blockHash)')
client.execute('INSERT INTO ethereum_miner_transaction (date,blockNumber,author,blockHash,rewardType,subtraces,value) VALUES', [{'date':1537768168,'blockNumber':0,'author':'some_user','blockHash':'some_hash','rewardType':'some_type','subtraces':0,'value':0.1}])
client.execute("INSERT INTO ethereum_miner_transaction (date,blockNumber,author,blockHash,rewardType,subtraces,value) VALUES (1537768168,0,'some_user5','j3kl45nt3g','coins',0,1.1),(1537768168,0,'some_user4','ajklnfjk3nq','coins',2,0.8),(1537768168,0,'some_user2','some hash346gfhds','coins',0,1.3)")
client.execute('SELECT * FROM ethereum_miner_transaction WHERE blockNumber = 0 ')
client.execute('SELECT * FROM ethereum_miner_transaction ORDER BY value DESC')



client.execute('DROP TABLE IF EXISTS ethereum_block')
 # id#number
client.execute('CREATE TABLE ethereum_block (date Date, number Int32, timestamp DateTime ) ENGINE = MergeTree() ORDER BY (number)')
client.execute('INSERT INTO ethereum_block (date,number,timestamp) VALUES', [{'date':1537768168,'number':0,'timestamp':1537768168},{'date':1537768168,'number':1,'timestamp':1537768168}])
client.execute('SELECT * FROM ethereum_block WHERE number = 1')
client.execute('SELECT * FROM ethereum_block ORDER BY number DESC')
#unix формат времени



# CREATE TABLE имя_таблицы ( имя_столбца тип_данных, имя_столбца тип_данных, имя_столбца тип_данных ) ENGINE = названия_движка

# INSERT INTO имя_таблицы (имя_столбца, имя_столбца) VALUES (значениие_для_первого_столбца, значениие_для_второго_столбца), (значениие_для_первого_столбца, значениие_для_второго_столбца), (значениие_для_первого_столбца, значениие_для_второго_столбца)

# INSERT INTO имя_таблицы (имя_столбца_1, имя_столбца_2) VALUES, [{'имя_столбца_1':значениие_для_первого_столбца, 'имя_столбца_1':значениие_для_второго_столбца}, {'имя_столбца_1':значениие_для_первого_столбца, 'имя_столбца_2':значениие_для_второго_столбца}, {'имя_столбца_1':значениие_для_первого_столбца, 'имя_столбца_2':значениие_для_второго_столбца}]  # для питон драйвера

# SELECT столбца_1, имя_столбца_2 FROM tимя_таблицы

# SELECT (столбца_1, имя_столбца_2) FROM tимя_таблицы


# DESC TABLE имя_таблицы FORMAT название_формата[JSON] #просмотр столбцов таблицы














