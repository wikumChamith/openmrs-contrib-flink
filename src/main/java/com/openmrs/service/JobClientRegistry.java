package com.openmrs.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for tracking running Flink job clients.
 * Allows cancellation of jobs by their database ID.
 */
@Slf4j
@Component
public class JobClientRegistry {

    private final ConcurrentHashMap<Integer, JobClient> clients = new ConcurrentHashMap<>();

    /**
     * Register a job client for tracking
     */
    public void register(Integer jobId, JobClient client) {
        clients.put(jobId, client);
        log.info("Registered JobClient for job ID: {}", jobId);
    }

    /**
     * Cancel and remove a job by its database ID
     */
    public void cancel(Integer jobId) {
        JobClient client = clients.remove(jobId);
        if (client == null) {
            log.warn("No JobClient found for job ID: {}. Job may have already stopped.", jobId);
            return;
        }

        try {
            client.cancel().get();
            log.info("Successfully cancelled Flink job for ID: {}", jobId);
        } catch (Exception e) {
            log.error("Failed to cancel Flink job for ID: {}", jobId, e);
        }
    }

    /**
     * Check if a job is registered
     */
    public boolean isRegistered(Integer jobId) {
        return clients.containsKey(jobId);
    }

    /**
     * Get count of registered jobs
     */
    public int getRegisteredCount() {
        return clients.size();
    }
}