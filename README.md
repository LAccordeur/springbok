This is the source code of Springbok core library. It includes the core code of indexing and storage for trajectory data management. 
We also implemented a lightweight HTTP server to provide HTTP API.

### Building from Source
Requirements:
- Git
- Java JDK 8
- Apache Maven 3.6.3 or later

Use Git to download the source code. Navigate to the destination directory, then run:
```
git clone https://github.com/LAccordeur/springbok.git
cd springbok
```

The project is build using Maven. To build, run:

```
mvn install -Dmaven.test.skip=true
```



### How to run the server
The main class of the server is located at the directory `src/main/java/com/rogerguo/test/server/SimpleSpringbokServer.java`. To start it, run:
```
mvn exec:java -Dexec.mainClass="com.rogerguo.test.server.SimpleSpringbokServer"
```

Note: create your credentials file under the directory ~/.aws/ before running the server.

### How to build the client
The client code can be found in the repository [`springbok-client`](https://github.com/LAccordeur/springbok-client-proj). It depends on the Springbok core library, so we first need to build the core library in this repository and then build the client.
```
git clone https://github.com/LAccordeur/springbok-client.git
cd springbok-client
mvn install -Dmaven.test.skip=true
```
### Benchmark
We use [JMH](https://openjdk.org/projects/code-tools/jmh/) as the benchmark util.
The benchmark code of Springbok core library can be found under the directory `src/main/java/com/rogerguo/test/benchmark`;
The benchmark code of end-to-end evaluation can be found in the repository [`springbok-benchmark`](https://github.com/LAccordeur/springbok-benchmark-proj). The dataset can be found in this [link](https://drive.google.com/drive/folders/1a7fiNfXNXneuDJ9-mEcZmBIsT1NVde1u?usp=share_link).

