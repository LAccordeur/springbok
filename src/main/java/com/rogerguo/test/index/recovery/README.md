usage:
- In the spatialtemporal tree, call *addFullLeafNode()* when a leaf node is full
- In the flush function, call *markBlockId()* when a block is flushed
- In the flush function, call *checkAndFlushLeafNode()* to flush and save checkpoint

when inserting data
- first create a LeafNodeStatusRecorder instance and specify the persistency layer (disk or object store)
- enable recovery flag in SpatialTemporalIndexTree and set recorder in the tree
- set recovery flag in ToDisk or ToS3 FlushPolicy
  - if disk layer is used, call setLeafNodeStatusRecorder() for toDiskFlushPolicy
  - if object store layer is used, call setLeafNodeStatusRecorder() for toS3FlushPolicy