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
        workerService.addWorker(worker);
        System.out.println("Worker registered!");
        System.out.println("Worker: " + worker);
    }

    @GetMapping("/list")
    public Map<String, WorkerInfo> listWorkers() {
        return workerService.getWorkers();
    }
}