package com.minicloud.sampo.worker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.io.IOException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WorkerApplication {
    public static void main(String[] args) throws IOException {
        String workerId = args[0];
        int workerPort = Integer.parseInt(args[1]);
        String workerStatus = "Online";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "worker");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

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

        try(KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            System.out.println("Worker " + workerId + " is listening for jobs...");
            consumer.subscribe(Collections.singletonList("job-queue"));

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.printf("Worker %s received message: %s%n", workerId, new String(record.value()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    } 
}

