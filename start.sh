echo 3 | sudo tee /proc/sys/vm/drop_caches
mvn exec:java -Dexec.mainClass="com.rogerguo.test.server.SimpleSpringbokServer" -Drun.jvmArguments="-Xmx20480m -Xms20480m"
