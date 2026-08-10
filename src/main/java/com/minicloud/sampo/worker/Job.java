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

    public Job(String id){
        this.id = id;
        this.status = "submitted";
        this.submittedAt = LocalDateTime.now();
    }

    protected Job() {
        // Default constructor for JPA
    }

    public void setStartTime(LocalDateTime startedAt){
        this.startedAt = startedAt;
    }

    public void setCompletedTime(LocalDateTime completedAt){
        this.completedAt = completedAt;
    }

    public void setExitCode(Integer exitCode){
        this.exitCode = exitCode;
    }

    public void setStatus(String status){
        this.status = status;
    }
}
