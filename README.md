# IndyDB
IndyDB is a simple log-based persistent key-value storage library.

## Usage

### Opening db
```
let db_params = DBParams::new();
let mut db = DB::open("newdb", db_params).unwrap();
```

### Reading value
```
let val = db.get("key").unwrap().unwrap();
```

### Writing value
```
db.put("key", "value").unwrap();
```
### Deleting value
```
db.delete("key").unwrap();
```
### Closing db
```
db.close().unwrap();
```
The drop trait calls this method when it goes out of scope, so you may not need to explicitly call this method.

## Benchmarks
TODO

## Design
IndyDB uses in-memory table (hashmap) to store key-value pairs. When the memtable exceeds a specified size (DBParmas.write_buffer_size) it is converted into an immutable flush-table.

A background thread flushes the flush-table to the disk as log files.
Corresponding to each mem_table a log table is created which consists of two files - data table and index table. For each key-value pair of the flush_table, the table builder adds the key and value marker (delete marker or data offset to the data table) to the index table and actual value to the data table. The keys and values are stored on the disk as : *data_len* *data_bytes*. The METADATA file of the db contains the number of log_tables in the db.
