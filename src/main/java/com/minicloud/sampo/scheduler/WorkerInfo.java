package com.minicloud.sampo.scheduler;

public class WorkerInfo {
    private String id;

    private String name;

    private int port;

    private String status;

    public WorkerInfo(String id, int port, String status) {
        this.id = id;
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
