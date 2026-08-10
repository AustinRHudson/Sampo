package com.minicloud.sampo.worker;

import com.minicloud.sampo.worker.Job;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobRepository extends JpaRepository<Job, String> {
    
}
