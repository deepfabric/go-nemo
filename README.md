# go-nemo
go wrapper for [nemo](https://github.com/Qihoo360/nemo);

Go-nemo is a storage engine for a consistent distributed kv storege named [elastic cell](https://github.com/deepfabric/elasticell).
# nemo
[nemo](https://github.com/Qihoo360/nemo) is a cpp library which encapsulates redis data structure such as k/v, hash, list, set, zset, persistented them on rocksdb. Nemo maps complex data structure to multiple kv entries in rocksdb as plain kv style storage. For instance ,a hash table is represtend as a meta record plus multi data record in rocksdb. Meta record consist of hash table name as rocksdb key, sum of hash table entries as rocksdb value. Data record is encoded with table name + table entry member as rocksdb key, table entry value as rocksdb value. 
# new feature
We add some new feature to nemo for our special use case.

- __storage volume info for complex data struture.__ In meta record, we not only keeps the sum of a hash table or set, but alse records the original storage volume before compresstion.

- __addiatinal rocksdb instances for extra data.__ We add a meta db for range info and a raft db for raft log.

- __data ingest api for online migrate.__ We use rocksdb 'ingest api' to bulk load external data into current nome instance.


 
