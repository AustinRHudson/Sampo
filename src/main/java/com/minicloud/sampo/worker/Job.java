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

    private Integer cpuLimit;

    private Integer memoryLimit;

    public Job(String id, Integer cpuLimit, Integer memoryLimit) {
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
        this.cpuLimit = 1;
        this.memoryLimit = 512;
    }

    protected Job() {
        // Default constructor for JPA
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
