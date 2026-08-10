package com.minicloud.sampo.scheduler;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
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

import com.minicloud.sampo.worker.Job;
import com.minicloud.sampo.worker.JobRepository;
import com.minicloud.sampo.scheduler.WorkerInfo;


@RestController
@CrossOrigin(origins = "http://localhost:5173")
@RequestMapping("/workers")
public class WorkerController {
    private final JobRepository jobRepository;

    private final WorkerService workerService;

    public WorkerController(WorkerService workerService, JobRepository jobRepository) {
        this.workerService = workerService;
        this.jobRepository = jobRepository;
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
        ProcessBuilder pb = new ProcessBuilder(
    "mvn",
    "exec:java",
    "-Dexec.mainClass=com.minicloud.sampo.worker.WorkerApplication",
    "-Dexec.args=" + worker.getId() + " " + worker.getPort()
);
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

    @GetMapping("/jobs")
    public List<Job> getJobs() {
        List<Job> jobs = jobRepository.findAll();
        System.out.println(jobs);
        System.out.println(jobRepository.findBy("status", "submitted"));
        return jobs;
    }

    @PostMapping("/job")
    public void receiveJob(
        @RequestParam("file") MultipartFile file,
        @RequestParam("jobId") String jobId,
        @RequestParam(value = "cpuLimit", required = false) Double cpuLimit,
        @RequestParam(value = "memoryLimitMb", required = false) Long memoryLimitMb) throws IOException {

        System.out.println("Received job with ID: " + jobId);
        Job job;
        if(cpuLimit != null && memoryLimitMb != null) {
            job = new Job(jobId, cpuLimit.intValue(), memoryLimitMb.intValue());
        } else {
            job = new Job(jobId);

        }
        jobRepository.save(job);
        System.out.println("Job saved to database with ID: " + jobId);
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
            job.setStatus("failed");
            job.setCompletedTime();
            job.setExitCode(-1);
            jobRepository.save(job);
        }
    }

    @PostMapping("/jobUpdate")
    public void updateJobStatus(@RequestBody Map<String, Object> payload) {
        String jobId = (String) payload.get("jobId");
        String status = (String) payload.get("status");
        String exitCode = (String) payload.get("exitCode");
        String startedAt = (String) payload.get("startedAt");
        String completedAt = (String) payload.get("completedAt");

        Job job = jobRepository.findById(jobId).orElse(null);
        if (job != null) {
            job.setStatus(status);
            if (startedAt.equals("1")) {
                job.setStartTime();
            } else if (completedAt.equals("1")) {
                job.setCompletedTime();
                job.setExitCode(Integer.parseInt(exitCode));
            }
            jobRepository.save(job);
            System.out.println("Job " + jobId + " updated to status: " + status);
        } else {
            System.err.println("Job with ID " + jobId + " not found.");
        }
    }
}