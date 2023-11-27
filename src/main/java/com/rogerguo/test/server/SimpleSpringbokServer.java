package com.rogerguo.test.server;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.common.SpatialBoundingBox;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.compression.StringCompressor;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.rogerguo.test.index.recovery.LeafNodeStatusRecorder;
import com.rogerguo.test.storage.driver.DiskDriver;
import com.rogerguo.test.storage.flush.S3LayoutSchemaName;
import com.rogerguo.test.store.Chunk;
import com.rogerguo.test.store.SeriesStore;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * @author yangguo
 * @create 2022-06-25 4:13 PM
 **/
public class SimpleSpringbokServer {

    static SeriesStore seriesStore;

    static HttpServer server;

    static ObjectMapper objectMapper = new ObjectMapper();

    //private static int compressionThreshold = 8 * 1024 * 1024; // 8 mb

    private static int compressionThreshold =  1024 * 1024 * 1024; // 1 GB

    static void initEmptySeriesStore() {

        seriesStore = SeriesStoreCreator.createEmptySeriesStore(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "springbok-store-csv-test", false);

        System.out.println("finish init");
    }

    static void startExistedSeriesStore() {

        seriesStore = SeriesStoreCreator.startExistedSeriesStore(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "springbok-store-csv-test-str", false);

        System.out.println("finish start");
    }

    static void startExistedSeriesStoreWithDiskCache() {
        seriesStore = SeriesStoreCreator.startExistedSeriesStoreWithDiskCacheFill(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "springbok-store-csv-test", false, 0, 0);

        System.out.println("finish start");
    }

    static void initEmptySeriesStoreWithDiskRecovery() {

        DiskDriver diskDriver = new DiskDriver("/home/ubuntu/data/recovery-test");
        LeafNodeStatusRecorder leafNodeStatusRecorder = new LeafNodeStatusRecorder(diskDriver);
        seriesStore = SeriesStoreCreator.createEmptySeriesStoreWithRecoveryAndDiskFlush(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "springbok-query", leafNodeStatusRecorder);

        System.out.println("finish init");
    }

    static void initAndIngestSeriesStore() {
        //String dataFile = "/home/ubuntu/data/porto_data_v1_1000w.csv";
        String dataFile = "/home/ubuntu/data1/porto_data_v1_45x.csv";
        seriesStore = SeriesStoreCreator.createAndFillSeriesStore(dataFile, S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-springbok-45x", true);
        System.out.println("finish init");
    }

    public static void main(String[] arg) throws Exception {
        System.setProperty("sun.net.httpserver.nodelay", "true");
        server = HttpServer.create(new InetSocketAddress(8001), 0);
        server.setExecutor(Executors.newFixedThreadPool(32));
        //server.setExecutor(Executors.newCachedThreadPool());
        initEmptySeriesStore();
        //startExistedSeriesStore();
        //startExistedSeriesStoreWithDiskCache();
        //startExistedSeriesStore();
        //initAndIngestSeriesStore();
        // pass query params through GET()
        server.createContext("/idtemporalquery", new SimpleSpringbokServer.IdTemporalQueryHandler());
        server.createContext("/insertion", new SimpleSpringbokServer.InsertionHandler());
        server.createContext("/spatialtemporalquery", new SimpleSpringbokServer.SpatialTemporalQueryHandler());
        server.createContext("/asyncinsertion", new SimpleSpringbokServer.AsyncInsertionHandler());
        server.createContext("/stop", new SimpleSpringbokServer.StopHandler());
        server.start();
        System.out.println("server is started");
    }

    static class StopHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {


            System.out.println("stopping server...");
            // seriesStore.stop();
            seriesStore.flushDataToDisk();

            String response = "successful";
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
            server.stop(0);
            System.out.println("stop springbok server successfully");
            System.exit(0);


        }
    }

    static class InsertionHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            /*String getString = httpExchange.getRequestURI().getQuery();
            System.out.println("get string: " + getString);*/

            String postString = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("data size: " + postString.length());

            List<TrajectoryPoint> pointList = objectMapper.readValue(postString, new TypeReference<List<TrajectoryPoint>>() {});
            for (TrajectoryPoint point : pointList) {
                seriesStore.appendSeriesPoint(point);
            }

            String response = "successful";
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

        }
    }

    static class AsyncInsertionHandler implements HttpHandler {

        static DataBuffer dataBuffer = new DataBuffer();

        static {
            System.out.println("init insertion handler for async");
            Thread consumer = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        dataBuffer.consume(seriesStore);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            });
            consumer.start();

        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {

            System.out.println(Thread.currentThread().getName());

            String getString = httpExchange.getRequestURI().getQuery();
            System.out.println("get string: " + getString);

            String postString = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("data size: " + postString.length());

            List<TrajectoryPoint> pointList = objectMapper.readValue(postString, new TypeReference<List<TrajectoryPoint>>() {});
            dataBuffer.produce(pointList);

            String response = "successful";
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

        }
    }

    static class DataBuffer {
        LinkedList<TrajectoryPoint> bufferList = new LinkedList<>();

        public void produce(List<TrajectoryPoint> list) {
            synchronized (this) {
                bufferList.addAll(list);
                System.out.println("produce: " + list.size() + " points");
                notify();
            }
        }

        public void consume(SeriesStore seriesStore) throws InterruptedException {
            while (true) {
                synchronized (this) {
                    if (bufferList.size() == 0) {
                        wait();
                    }
                    for (TrajectoryPoint point : bufferList) {
                        seriesStore.appendSeriesPoint(point);
                    }
                    System.out.println("consume: " + bufferList.size() + " points");
                    bufferList.clear();
                    notify();
                }
            }
        }
    }

    static String serializeResult(List<TrajectoryPoint> pointList) {
        StringBuilder stringBuilder = new StringBuilder();
        for (TrajectoryPoint point : pointList) {
            //String string = String.format("%s;%f;%f;%d;%s\n", point.getOid(), point.getLongitude(), point.getLatitude(), point.getTimestamp(), point.getPayload());
            //String string = point.getPayload() + "\n";
            //String string = point.getOid() + "," + point.getLongitude() + "," + point.getLatitude() + "," + point.getTimestamp() + "," + point.getPayload();
            stringBuilder.append(point.getPayload()).append(";");
        }

        return stringBuilder.toString();
    }


    static class SpatialTemporalQueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            // from get
            String queryString = httpExchange.getRequestURI().getQuery();
            System.out.println("get string: " + queryString);

            // from post
            String postString = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("post string: " + postString);

            SpatialTemporalRangeQueryPredicate predicate = objectMapper.readValue(postString, SpatialTemporalRangeQueryPredicate.class);
            System.out.println("predicate: " + predicate);

            long startQuery = System.currentTimeMillis();
            List<TrajectoryPoint> result = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            long stopQuery = System.currentTimeMillis();

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            //String resultString = objectMapper.writeValueAsString(result);
            System.out.println("result point number: " + result.size());
            long startReturn = System.currentTimeMillis();
            String resultString = serializeResult(result);
            if (resultString.length() < compressionThreshold) {
                System.out.println("raw network data\n\n");
                os.write((resultString).getBytes(StandardCharsets.UTF_8));
            } else {
                System.out.println("compress network data\n\n");
                byte[] compressed = StringCompressor.snappyCompressString(resultString);
                os.write(compressed);
                System.out.println("before compression size: " + resultString.length() + ", after compression: " + compressed.length);
            }
            long stopReturn = System.currentTimeMillis();
            os.close();
            System.out.println("[server] query layer time: " + (stopQuery - startQuery));
            System.out.println("[server] serilization and return data to client time: " + (stopReturn - startReturn));

        }
    }

    static class IdTemporalQueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // from get
            String queryString = exchange.getRequestURI().getQuery();
            System.out.println("get string: " + queryString);

            // from post
            String postString = IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("post string: " + postString);

            IdTemporalQueryPredicate predicate = objectMapper.readValue(postString, IdTemporalQueryPredicate.class);
            System.out.println("predicate: "  + predicate );

            long startQuery = System.currentTimeMillis();
            List<TrajectoryPoint> result = seriesStore.idTemporalQueryWithRefinement(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());
            long stopQuery = System.currentTimeMillis();

            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            //String resultString = objectMapper.writeValueAsString(result);
            long startReturn = System.currentTimeMillis();
            String resultString = serializeResult(result);
            if (resultString.length() < compressionThreshold) {
                os.write((resultString).getBytes(StandardCharsets.UTF_8));
            } else {
                System.out.println("compress network data\n\n");
                byte[] compressed = StringCompressor.snappyCompressString(resultString);
                os.write(compressed);
                System.out.println("before compression size: " + resultString.length() + ", after compression: " + compressed.length);

            }
            long stopReturn = System.currentTimeMillis();
            os.close();
            System.out.println("[server] query layer time: " + (stopQuery - startQuery));
            System.out.println("[server] serilization and return data to client time: " + (stopReturn - startReturn));
        }
    }

}
