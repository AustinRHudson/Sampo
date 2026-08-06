package com.minicloud.sampo;

import com.Worker;

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
    public void registerWorker(@RequestBody Worker worker) {
        workerService.addWorker(worker);
    }

    @GetMapping("/list")
    public Map<String, Worker> listWorkers() {
        return workerService.getWorkers();
    }
}