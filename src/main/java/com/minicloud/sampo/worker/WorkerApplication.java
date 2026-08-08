package com.minicloud.sampo.worker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.io.IOException;

public class WorkerApplication {
    public static void main(String[] args) throws IOException {
        String workerId = args[0];
        int workerPort = Integer.parseInt(args[1]);
        String workerStatus = "Online";

        HttpServer server = HttpServer.create(new InetSocketAddress(workerPort), 0);

        server.createContext("/status", exchange -> {
            String response = "Worker ID: " + workerId + ", Port: " + workerPort + ", Status: " + workerStatus;
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        });

        server.start();
        System.out.println("Server started on port " + workerPort);
    } 
}

