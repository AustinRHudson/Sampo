package com.minicloud.sampo.scheduler;

public class WorkerInfo {
    private String id;

    private int port;

    private String status;

    private static int idIncrementer = 0;

    public WorkerInfo(String id, int port, String status) {
        this.id = "worker-" + idIncrementer++;
        this.port = port;
        this.status = status;
    }

    public String getId() {
        return this.id;
    }

    public int getPort() {
        return this.port;
    }

    public String getStatus() {
        return this.status;
    }
}
