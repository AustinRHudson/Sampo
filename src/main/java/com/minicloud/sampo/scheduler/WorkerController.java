package com.minicloud.sampo.scheduler;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/workers")
public class WorkerController {
    private final WorkerService workerService;

    public WorkerController(WorkerService workerService) {
        this.workerService = workerService;
    }

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
                    java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
                    java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create("http://localhost:" + worker.getPort() + "/status"))
                            .build();
                    java.net.http.HttpResponse<String> response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
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
}