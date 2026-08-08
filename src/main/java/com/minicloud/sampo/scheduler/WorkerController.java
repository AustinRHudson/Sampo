package com.minicloud.sampo.scheduler;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
@RequestMapping("/workers")
public class WorkerController {
    private final WorkerService workerService;

    public WorkerController(WorkerService workerService) {
        this.workerService = workerService;
    }

    @PostMapping("/register")
    public void registerWorker(@RequestBody WorkerInfo worker) {
        if(workerService.getWorkers().containsKey(worker.getId())) {
            System.out.println("Worker already registered!");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/list")
    public Map<String, WorkerInfo> listWorkers() {
        return workerService.getWorkers();
    }
}