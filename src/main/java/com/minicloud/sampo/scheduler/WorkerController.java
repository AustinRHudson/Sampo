package com.minicloud.sampo.scheduler;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.net.http.*;
import java.nio.file.Files;
import java.net.URI;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import java.util.Properties;


@RestController
@CrossOrigin(origins = "http://localhost:5173")
@RequestMapping("/workers")
public class WorkerController {
    private final WorkerService workerService;

    public WorkerController(WorkerService workerService) {
        this.workerService = workerService;
    }

    Properties props = new Properties();
    {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    }

    private final HttpClient client = java.net.http.HttpClient.newHttpClient();

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @PostMapping("/register")
    public void registerWorker(@RequestBody WorkerInfo worker) {
        if(workerService.getWorkers().containsKey(String.valueOf(worker.getPort()))) {
            System.out.println("This port is in use!");
            return;
        }
        workerService.addWorker(worker);
        System.out.println("Worker registered!");
        System.out.println("Worker: " + worker.getId() + ", Port: " + worker.getPort() + ", Status: " + worker.getStatus());
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "target/classes",
         "com.minicloud.sampo.worker.WorkerApplication", worker.getId(), String.valueOf(worker.getPort()));
         pb.inheritIO(); 
        try {
            pb.start();
            executorService.scheduleAtFixedRate(() -> {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + worker.getPort() + "/status"))
                            .build();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    System.out.println(response.body());
                } catch (Exception e) {
                    System.out.println("Worker " + worker.getId() + " is offline.");
                }
            }, 5, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/list")
    public Map<String, WorkerInfo> listWorkers() {
        return workerService.getWorkers();
    }

    @PostMapping("/job")
    public void receiveJob(@RequestParam("file") MultipartFile file, @RequestParam("jobId") String jobId) throws IOException {
        System.out.println("Received job with ID: " + jobId);
        try (Producer<String, byte[]> producer = new KafkaProducer<>(props)) {
            System.out.println("Reading ZIP payload from disk...");
            byte[] zipBytes = file.getBytes();

            System.out.printf("Sending Job [%s] (%d bytes) to Kafka...%n", jobId, zipBytes.length);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("job-queue", jobId, zipBytes);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.printf("Successfully queued! Partition: %d, Offset: %d%n", 
                                metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("Failed to publish zip file to queue: " + exception.getMessage());
                    }
                }
            });
            producer.flush();

        } catch (IOException e) {
            System.err.println("Error reading the local ZIP file: " + e.getMessage());
        }
    }
}