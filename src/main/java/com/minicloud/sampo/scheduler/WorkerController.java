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

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import java.util.Properties;

import com.minicloud.sampo.worker.Job;
import com.minicloud.sampo.worker.JobRepository;


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

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @PostMapping("/register")
    public void registerWorker() {
        WorkerInfo worker = new WorkerInfo();
        workerService.addWorker(worker);
        System.out.println("Worker registered!");
        System.out.println("Worker: " + worker.getId() + ", Port: " + worker.getPort() + ", Status: " + worker.getStatus());
        workerService.startWorker(worker);
    }

    @GetMapping("/list")
    public Map<String, WorkerInfo> listWorkers() {
        //System.out.println("Listing workers..." + workerService.getWorkers());
        return workerService.getWorkers();
    }

    @GetMapping("/jobs")
    public List<Job> getJobs() {
        List<Job> jobs = jobRepository.findAll();
        return jobs;
    }

    @PostMapping("/job")
    public void receiveJob(
        @RequestParam("file") MultipartFile file,
        @RequestParam(value = "cpuLimit", required = false) Double cpuLimit,
        @RequestParam(value = "memoryLimitMb", required = false) Integer memoryLimitMb) throws IOException {
        Job job;
        if(cpuLimit != null && memoryLimitMb != null) {
            job = new Job(cpuLimit.doubleValue(), memoryLimitMb.intValue());
        } else {
            job = new Job();
        }
        System.out.println("Received job with ID: " + job.getId());
        jobRepository.save(job);
        System.out.println("Job saved to database with ID: " + job.getId());
        try (Producer<String, byte[]> producer = new KafkaProducer<>(props)) {
            System.out.println("Reading ZIP payload from disk...");
            byte[] zipBytes = file.getBytes();

            System.out.printf("Sending Job [%s] (%d bytes) to Kafka...%n", job.getId(), zipBytes.length);
            String jobInfo = job.getId() + " " + job.getCpuLimit() + " " + job.getMemoryLimit();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("job-queue", jobInfo, zipBytes);

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
        Integer exitCode = null;
        Object exitCodeObj = payload.get("exitCode");
        if (exitCodeObj instanceof Number) {
            exitCode = ((Number) exitCodeObj).intValue();
        } else if (exitCodeObj instanceof String) {
            try {
                exitCode = Integer.valueOf((String) exitCodeObj);
            } catch (NumberFormatException ignored) {
            }
        }
        String startedAt = (String) payload.get("startedAt");
        String completedAt = (String) payload.get("completedAt");

        Job job = jobRepository.findById(jobId).orElse(null);
        if (job != null) {
            job.setStatus(status);
            if (startedAt.equals("1")) {
                job.setStartTime();
            } else if (completedAt.equals("1")) {
                job.setCompletedTime();
                job.setExitCode(exitCode);
            }
            jobRepository.save(job);
            System.out.println("Job " + jobId + " updated to status: " + status);
        } else {
            System.err.println("Job with ID " + jobId + " not found.");
        }
    }
}