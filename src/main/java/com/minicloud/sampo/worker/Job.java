package com.minicloud.sampo.worker;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "jobs")

public class Job {
    @Id

    private String id;

    private String status;

    private LocalDateTime submittedAt;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private Integer exitCode;

    private Double cpuLimit;

    private Integer memoryLimit;

    public Job(String id, Double cpuLimit, Integer memoryLimit) {
        this.id = id;
        this.status = "submitted";
        this.submittedAt = LocalDateTime.now();
        this.exitCode = -1;
        this.cpuLimit = cpuLimit;
        this.memoryLimit = memoryLimit;
    }

    public Job(String id) {
        this.id = id;
        this.status = "submitted";
        this.submittedAt = LocalDateTime.now();
        this.exitCode = -1;
        this.cpuLimit = 1.0;
        this.memoryLimit = 512;
    }

    protected Job() {
        // Default constructor for JPA
    }

     public String getId() {
        return id;
    }

    public String getStatus() {
        return status;
    }

    public Double getCpuLimit() {
        return cpuLimit;
    }

    public Integer getMemoryLimit() {
        return memoryLimit;
    }

    public LocalDateTime getSubmittedAt() {
        return submittedAt;
    }

    public LocalDateTime getStartedAt() {
        return startedAt;
    }

    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public void setStartTime(){
        this.startedAt = LocalDateTime.now();
    }

    public void setCompletedTime(){
        this.completedAt = LocalDateTime.now();
    }

    public void setExitCode(Integer exitCode){
        this.exitCode = exitCode;
    }

    public void setStatus(String status){
        this.status = status;
    }
}
