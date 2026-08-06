package com.minicloud.sampo;

import org.springframework.stereotype.Service;

import com.Worker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WorkerService {
    private Map<String, Worker> workers = new ConcurrentHashMap<>();

    public void addWorker(Worker worker) {
        workers.put(worker.getId(), worker);
    }

    public Map<String, Worker> getWorkers() {
        return workers;
    }
}
