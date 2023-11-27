TiredCloudStorageManager
  - LocationMappingTable: based on block id, tell us the storage location of this block
  - Map<String, StorageLayer>: The existing storage layers. key is a storage layer name and value is corresponding storage layer. 
  - StorageLocationDecider

StorageLayer
  - LocalLocationMappingTable: the blocks that in this storage layer
  - CostPerformanceModel
  - PersistenceDriver
  - FlushPolicy
  - StorageUsage
  - LastFlushTime
  - isNeedFlush()

process
1. append block to manager
2. then check each layer and find the one needed flush




TODO
- EC2 exp
- storage layer optimization (S3)  
- asyn flush  
- concurrency
- index optimization: append only