package com.minicloud.sampo.scheduler;

public class WorkerInfo {
    private String id;

    private int port;

    private String status;

    private static int idIncrementer = 0;

    private static int portIncrementer = 9000;

    private int activeJobs;

    private int maxCPU;

    private int maxMemory;

    public WorkerInfo() {
        this.id = "worker-" + idIncrementer++;
        this.port = portIncrementer++;
        this.status = "ONLINE";
        this.activeJobs = 0;
        this.maxCPU = 2;
        this.maxMemory = 1024;
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
        activeJobs -= 1;
    }

    public int getMaxCPU(){
        return this.maxCPU;
    }

    public int getMaxmemory(){
        return this.maxMemory;
    }
}
