package com.minicloud.sampo.scheduler;

import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.net.http.*;

@Service
public class WorkerService {
    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    private final Map<String, Process> workerProcesses = new ConcurrentHashMap<>();

    private final HttpClient client = java.net.http.HttpClient.newHttpClient();

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    public void addWorker(WorkerInfo worker) {
        workers.put(String.valueOf(worker.getPort()), worker);
    }

    public void startWorker(WorkerInfo worker){
        ProcessBuilder pb = new ProcessBuilder(
            "mvn",
            "exec:java",
            "-Dexec.mainClass=com.minicloud.sampo.worker.WorkerApplication",
            "-Dexec.args=" + worker.getId() + " " + worker.getPort() + " " + worker.getMaxCPU() + " " + worker.getMaxmemory()
        );
        pb.inheritIO(); 
        try {
            Process process = pb.start();
            workerProcesses.put(String.valueOf(worker.getPort()), process);
            executorService.scheduleAtFixedRate(() -> {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + worker.getPort() + "/status"))
                            .build();
                    client.send(request, HttpResponse.BodyHandlers.ofString());
                    //System.out.println(response.body());
                } catch (Exception e) {
                    System.out.println("Worker " + worker.getId() + " is offline.");
                }
            }, 5, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("Error detected: ");
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void shutdown() {
        workerProcesses.values().forEach(process -> {
            if (process.isAlive()) {
                process.destroy();
            }
        });
    }

    public Map<String, WorkerInfo> getWorkers() {
        return workers;
    }
}
