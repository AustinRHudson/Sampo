package com.minicloud.sampo.scheduler;

public class WorkerInfo {
    private String id;

    private int port;

    private String status;

    private static int idIncrementer = 0;

    private static int portIncrementer = 9000;

    private int activeJobs;

    public WorkerInfo() {
        this.id = "worker-" + idIncrementer++;
        this.port = portIncrementer++;
        this.status = "ONLINE";
        this.activeJobs = 0;
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

    public void incrementJobs(){
        activeJobs += 1;
    }

    public void decrementJobs(){
        activeJobs -= 1
    }
}
