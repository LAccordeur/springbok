java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "GeoHash.*Benchmark" -o ./benchmark-log/headchunk/geohash.log
java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "Semi.*Benchmark" -o ./benchmark-log/headchunk/semi.log
java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "Phy.*Benchmark" -o ./benchmark-log/headchunk/phy.log
java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "RStar.*Benchmark" -o ./benchmark-log/headchunk/rstar.log
java -Xms4096m -Xmx8192m -jar target/microbenchmarks.jar "RTree.*Benchmark" -o ./benchmark-log/headchunk/rtree.log