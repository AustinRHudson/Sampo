package com.minicloud.sampo.scheduler;

public class WorkerInfo {
    private String id;

    private String name;

    private int port;

    private String status;

    public WorkerInfo(String id, String name, int port, String status) {
        this.id = id;
        this.name = name;
        this.port = port;
        this.status = status;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }
}
