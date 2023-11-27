package com.rogerguo.test.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * @author yangguo
 * @create 2022-06-23 10:25 AM
 **/
public class ServerDemo {

    public static void main(String[] arg) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8001), 0);
        server.createContext("/test", new TestHandler());
        server.start();
    }

    static class TestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // from get
            String queryString = exchange.getRequestURI().getQuery();
            System.out.println("get string: " + queryString);
            // from post
            String postString = IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("post string: " + postString);

            String response = "hello world";
            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

}
