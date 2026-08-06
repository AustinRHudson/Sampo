package com.minicloud.sampo;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/workers")
public class WorkerController {
    private final workerService workerService;

    @PostMapping("/register")
    public void registerWorker(@RequestBody Worker worker) {
        workerService.addWorker(worker);
    }

    @GetMapping("/list")
    public Map<String, Worker> listWorkers() {
        return workerService.getWorkers();
    }