 ## Steps for migrating the archiver database v1 to v2
 

> It is strongly advised to keep a backup copy of your database for the purposes of migration. The `go run` command below will modify the database irreversibly.

1. Run `go run github.com/cockroachdb/pebble/cmd/pebble@v1.1.5 db upgrade <db-dir>` to update the database format to a newer pebble supported version. 
2. Run `./archiver-db-migrator --database-path-old <old-db-dir> --database-path-new <new-db-dir>` for info about the input database.
3. To migrate a singular epoch run `./archiver-db-migrator --database-path-old <old-db-dir> --database-path-new <new-db-dir> --migrate-epoch <epoch-number>`.
4. To migrate a range of epochs run `./archiver-db-migrator --database-path-old <old-db-dir> --database-path-new <new-db-dir> --migrate-epoch-range-start <epoch-number> --migrate-epoch-range-end <epoch-number>`.
5. To migrate all the epochs run `/archiver-db-migrator --database-path-old <old-db-dir> --database-path-new <new-db-dir> --migrate-all true`

> After migration, the data may not be fully organized, resulting in a larger storage footprint.  
> You can add the `--database-compact-after-migrate` flag to force database compaction at migration time.  
> Note that this may increase the migration time significantly.

```
archiver-db-migrator [options...] [arguments...]

OPTIONS
      --batch-size                      <int>     (default: 10000)        
      --database-compact-after-migrate  <bool>    (default: false)        
      --database-path-new               <string>  (default: storage/new)  
      --database-path-old               <string>  (default: storage/old)  
  -h, --help                                                              display this help message
      --migrate-all                     <bool>    (default: false)        
      --migrate-epoch                   <uint>    (default: 0)            
      --migrate-epoch-range-end         <uint>    (default: 0)            
      --migrate-epoch-range-start       <uint>    (default: 0)            

ENVIRONMENT
  ARCHIVER_MIGRATOR_V2_BATCH_SIZE                      <int>     (default: 10000)        
  ARCHIVER_MIGRATOR_V2_DATABASE_COMPACT_AFTER_MIGRATE  <bool>    (default: false)        
  ARCHIVER_MIGRATOR_V2_DATABASE_PATH_NEW               <string>  (default: storage/new)  
  ARCHIVER_MIGRATOR_V2_DATABASE_PATH_OLD               <string>  (default: storage/old)  
  ARCHIVER_MIGRATOR_V2_MIGRATE_ALL                     <bool>    (default: false)        
  ARCHIVER_MIGRATOR_V2_MIGRATE_EPOCH                   <uint>    (default: 0)            
  ARCHIVER_MIGRATOR_V2_MIGRATE_EPOCH_RANGE_END         <uint>    (default: 0)            
  ARCHIVER_MIGRATOR_V2_MIGRATE_EPOCH_RANGE_START       <uint>    (default: 0
```