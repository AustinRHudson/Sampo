package com.minicloud.sampo.worker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class WorkerApplication {

    private static final ExecutorService jobExecutor = Executors.newFixedThreadPool(1);
    public static void main(String[] args) throws IOException {
        String workerId = args[0];
        int workerPort = Integer.parseInt(args[1]);
        String workerStatus = "ONLINE";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "worker");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        HttpServer server = HttpServer.create(new InetSocketAddress(workerPort), 0);
        HttpClient client = java.net.http.HttpClient.newHttpClient();

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
                    System.out.printf("Worker received message:", workerId);
                    String[] jobData = record.key().split(" ");
                    int jobId = Integer.parseInt(jobData[0]);
                    double cpu = Double.parseDouble(jobData[1]);
                    long memory = Long.parseLong(jobData[2]);
                    String json = "{" +
                            "\"jobId\": \"" + jobId + "\"," +
                            "\"status\": \"running\"," +
                            "\"submittedAt\": \"0\"," +
                            "\"startedAt\": \"1\"," +
                            "\"completedAt\": \"0\"," +
                            "\"exitCode\": \"-1\"" +
                            "}";
                    sendJobUpdate(json, client);
                    byte[] fileData = record.value();
                    if(fileData != null) {
                        Path outputDir = Path.of("output"); 
                        String fileName = unzipFromByteArray(fileData, outputDir); 
                        Path jobPath = Path.of("output", fileName); 
                        Path dockerPath = Path.of("output", fileName, "Dockerfile");
                        ProcessBuilder pb = new ProcessBuilder(
                            "docker",
                            "build",
                            "-t",
                            fileName.toLowerCase(),
                            "-f",
                            dockerPath.toString(),
                            jobPath.toString()
                        );

                        pb.inheritIO();

                        pb.start().waitFor(); // Wait for the Docker build to complete

                        String memoryLimit = "--memory=" + memory + "m";
                        String cpuLimit = "--cpus=" + cpu;

                        pb = new ProcessBuilder(
                            "docker",
                            "run",
                            "--rm",
                            "--name",
                            "sampo-job-" + jobId,
                            memoryLimit,
                            cpuLimit,
                            fileName.toLowerCase()
                        );

                        pb.inheritIO();

                        Process process = pb.start();

                        jobExecutor.submit(() -> {
                            try {
                                Path deleteDir = Path.of("output", fileName);
                                int exitCode = process.waitFor();

                                String status = (exitCode == 0)
                                        ? "completed"
                                        : "failed";

                                String jsonUpdate = "{" +
                                        "\"jobId\": \"" + jobId + "\"," +
                                        "\"workerId\": \"" + workerId + "\"," +
                                        "\"status\": \"" + status + "\"," +
                                        "\"submittedAt\": \"0\"," +
                                        "\"startedAt\": \"0\"," +
                                        "\"completedAt\": \"1\"," +
                                        "\"exitCode\": " + exitCode +
                                        "}";

                                sendJobUpdate(jsonUpdate, client);

                                deleteDirectory(deleteDir.toString());

                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                System.err.println("Job was interrupted.");
                            }
                        });
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    } 

     private static String unzipFromByteArray(byte[] bytes, Path targetDir) {
        // Stream straight from the byte array without creating a temporary file
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ZipInputStream zis = new ZipInputStream(bais)) {

            ZipEntry entry;

            Path firstExtractedRoot = null; // To track the first extracted root directory
            while ((entry = zis.getNextEntry()) != null) {
                Path filePath = targetDir.resolve(entry.getName()).normalize();

                if (firstExtractedRoot == null) {
                    // This extracts just the first part of the relative path inside targetDir
                    Path relativePath = targetDir.relativize(filePath);
                    firstExtractedRoot = targetDir.resolve(relativePath.getName(0));
                }

                System.out.println(filePath.toString());

                // // Security Check: Guard against Zip Slip attacks
                // if (!filePath.startsWith(targetDir)) {
                //     throw new IOException("Malicious entry outside target directory: " + entry.getName());
                // }

                if (entry.isDirectory()) {
                    Files.createDirectories(filePath);
                } else {
                    Files.createDirectories(filePath.getParent());
                    try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
                        zis.transferTo(fos); // Java 9+ feature to stream directly
                    }
                }
                zis.closeEntry();
            }
                System.out.println("Successfully unpacked ZIP received from Kafka!");
                return firstExtractedRoot != null ? firstExtractedRoot.getFileName().toString() : null; // Return the name of the last file processed
        } catch (IOException e) {
            System.err.println("Failed to unpack payload: " + e.getMessage());
            return null;
        }
    }

     public static void deleteDirectory(String folderPath) {
        if (folderPath == null) return;
        
        Path pathToBeDeleted = Paths.get(folderPath);

        if (!Files.exists(pathToBeDeleted)) {
            System.out.println("Directory does not exist, no deletion needed: " + folderPath);
            return;
        }

        try (Stream<Path> walk = Files.walk(pathToBeDeleted)) {
            walk.sorted(Comparator.reverseOrder()) // Reverse order puts children before parents
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        System.err.println("Failed to delete: " + path + " -> " + e.getMessage());
                    }
                });
            System.out.println("Successfully deleted directory and all contents: " + folderPath);
        } catch (IOException e) {
            System.err.println("Failed to walk the directory for deletion: " + e.getMessage());
        }
    }

    /*
    Template for json
    "{
        \"jobId\": \" + record.key() + \",
        \"status\": \"started\",
        \"submittedAt\": \"1\",
        \"startedAt\": \"0\",
        \"completedAt\": \"0\",
        \"exitCode\": \"0\"

    }"    
    */
    public static void sendJobUpdate(String json, HttpClient client) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/workers/jobUpdate"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
                client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                System.out.println("Failed to send job update to scheduler: " + e.getMessage());
            }
        }
}

