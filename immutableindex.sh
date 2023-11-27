java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "TreeIdTemporalQueryBenchmark" -o ./benchmark-log/immutablechunk/treeidtemporal.log
java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "TreeInsertionBenchmark" -o ./benchmark-log/immutablechunk/treeinsertion.log
java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "TreeSpatialTemporalQueryBenchmark" -o ./benchmark-log/immutablechunk/treespatialtemporal.log
