# The qubic archiver db migration tool

> The main purpose of this tool is to migrate the archiver database to a new format, which takes less space and enables compression.

## Configuration

The migration tool allows the user to specify, if required, which information to be included in the migrated version of the database.  
While excluding certain information is possible, it is not recommended as it may cause archiver to stop processing ticks due to missing information. 
If you decide to exclude certain tables, make sure to **BACK UP** your previous database.  

An option to not migrate the quorum data structure to the new format is also available.
Please **MAKE SURE** that archiver is configured to work with the new format (which it is by default).

The tool is also capable of migrating between Snappy (PebbleDB default) and Zstd compressions.

```bash
  --migration-tick-data / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_TICK_DATA                                                                  <bool>    (default: true)
  --migration-quorum-data / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_QUORUM_DATA                                                              <bool>    (default: true)
  --migration-computor-list / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_COMPUTOR_LIST                                                          <bool>    (default: true)
  --migration-transactions / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_TRANSACTIONS                                                            <bool>    (default: true)
  --migration-transaction-status / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_TRANSACTION_STATUS                                                <bool>    (default: true)
  --migration-last-processed-tick / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_LAST_PROCESSED_TICK                                              <bool>    (default: true)
  --migration-last-processed-tick-per-epoch / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_LAST_PROCESSED_TICK_PER_EPOCH                          <bool>    (default: true)
  --migration-skipped-ticks-interval / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_SKIPPED_TICKS_INTERVAL                                        <bool>    (default: true)
  --migration-identity-transfer-transactions / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_IDENTITY_TRANSFER_TRANSACTIONS                        <bool>    (default: true)
  --migration-chain-digest / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_CHAIN_DIGEST                                                            <bool>    (default: true)
  --migration-processed-tick-intervals / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_PROCESSED_TICK_INTERVALS                                    <bool>    (default: true)
  --migration-tick-transaction-status / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_TICK_TRANSACTION_STATUS                                      <bool>    (default: true)
  --migration-store-digest / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_STORE_DIGEST                                                            <bool>    (default: true)
  --migration-empty-ticks-per-epoch / $QUBIC_ARCHIVER_DB_MIGRATOR_MIGRATION_EMPTY_TICKS_PER_EPOCH                                          <bool>    (default: true)
  
  --options-migrate-quorum-data-to-v-2 / $QUBIC_ARCHIVER_DB_MIGRATOR_OPTIONS_MIGRATE_QUORUM_DATA_TO_V_2                                    <bool>    (default: true)
  
  --database-old-path / $QUBIC_ARCHIVER_DB_MIGRATOR_DATABASE_OLD_PATH                                                                      <string>  (default: ./storage/old)
  --database-old-compression / $QUBIC_ARCHIVER_DB_MIGRATOR_DATABASE_OLD_COMPRESSION                                                        <string>  (default: Snappy)
  
  --database-new-path / $QUBIC_ARCHIVER_DB_MIGRATOR_DATABASE_NEW_PATH                                                                      <string>  (default: ./storage/new/zstd)
  --database-new-compression / $QUBIC_ARCHIVER_DB_MIGRATOR_DATABASE_NEW_COMPRESSION                                                        <string>  (default: Zstd)

  --database-new-better-compaction / $QUBIC_ARCHIVER_DB_MIGRATOR_DATABASE_NEW_BETTER_COMPACTION                                            <bool>    (default: false)

  --export-export-last-tick-quorum-data-from-old-format / $QUBIC_ARCHIVER_DB_MIGRATOR_EXPORT_EXPORT_LAST_TICK_QUORUM_DATA_FROM_OLD_FORMAT  <bool>    (default: false)
  --export-import-last-tick-quorum-data-from-new-format / $QUBIC_ARCHIVER_DB_MIGRATOR_EXPORT_IMPORT_LAST_TICK_QUORUM_DATA_FROM_NEW_FORMAT  <bool>    (default: false)

  
  --help / -h  

```


## Usage example

- Old database path: `./archiver/store/archiver`
- Desired migrated path: `./archiver/store/new`

### Simple Archiver v0.x.x to to v1.x.x migration
```bash
./archiver-db-migrator --database-old-path ./archiver/store/archiver --database-new-path ./archiver/store/new
```

### Snappy to Snappy with new quorum data format
```bash
./archiver-db-migrator --database-old-path ./archiver/store/archiver --database-new-path ./archiver/store/new --database-new-compression Snappy
```

### Snappy to ZSTD without new quorum data format
```bash
./archiver-db-migrator --database-old-path ./archiver/store/archiver --database-new-path ./archiver/store/new --options-migrate-quorum-data-to-v-2 false
```

## New Last Quorum Data Per Epoch Interval format
Version `v0.9.0` of the Archiver introduces a new format for how the quorum data of the last tick of an interval is stored.  
This release fixes the issue where Archiver `v0.8.x` would store and report the quorum data of the last tick of the epoch when querried for the quorum data of the last tick on an interval in the case of multi interval epoch.
Unforunately, due to this issue, **your instance may be missing** the last tick quorum data for the multi interval epochs. 
In order to resolve this, you will have to import the missing data using the following steps:
1. Download the tar archive containing the [missing data](https://github.com/qubic/archiver-db-migrator/releases/tag/v0.1.0).
2. Extract the data:
```bash
tar -xvzf quorum-data-epochs-104-136.tar.gz 
```
The information will be extracted in the format of a qubic database directory called `extracted`.  
3. Run the migrator command to import the information into your database:
```bash
./archiver-db-migrator --database-old-path extracted --database-old-compression Zstd --database-new-path <database-path> --export-import-last-tick-quorum-data-from-new-format true
```
Make sure to replace `<database-path>` with the path of the directory of your database.  
4. You can now start archiver with release v0.9.0

