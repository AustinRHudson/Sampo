package com.minicloud.sampo.scheduler;

public class WorkerInfo {
    private String id;

    private int port;

    private String status;

    private static int idIncrementer = 0;

    private static int portIncrementer = 9000;

    public WorkerInfo() {
        this.id = "worker-" + idIncrementer++;
        this.port = portIncrementer++;
        this.status = "ONLINE";
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
