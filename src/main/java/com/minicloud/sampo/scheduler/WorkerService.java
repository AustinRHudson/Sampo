package com.minicloud.sampo.scheduler;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WorkerService {
    private Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    public void addWorker(WorkerInfo worker) {
        workers.put(String.valueOf(worker.getPort()), worker);
    }

    public Map<String, WorkerInfo> getWorkers() {
        return workers;
    }
}
