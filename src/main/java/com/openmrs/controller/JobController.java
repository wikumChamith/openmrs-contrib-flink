package com.openmrs.controller;

import com.openmrs.model.Job;
import com.openmrs.service.FlinkJobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final FlinkJobService flinkJobService;

    /**
     * Get all jobs (passwords are masked in response)
     */
    @GetMapping
    public ResponseEntity<List<Job>> getAllJobs() {
        try {
            List<Job> jobs = flinkJobService.getAllJobs();
            jobs.forEach(this::maskPasswords);
            return ResponseEntity.ok(jobs);
        } catch (Exception e) {
            log.error("Error retrieving jobs", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    private void maskPasswords(Job job) {
        if (job.getSource() != null && job.getSource().getSourcePassword() != null) {
            job.getSource().setSourcePassword("******");
        }
        if (job.getSink() != null && job.getSink().getSinkPassword() != null) {
            job.getSink().setSinkPassword("******");
        }
    }

    /**
     * Upload YAML file to register a new Flink CDC ETL job
     */
    @PostMapping("/upload")
    public ResponseEntity<?> uploadYamlFile(@RequestParam("file") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("File is empty"));
            }

            String filename = file.getOriginalFilename();
            if (filename == null || (!filename.endsWith(".yaml") && !filename.endsWith(".yml"))) {
                return ResponseEntity.badRequest()
                        .body(createErrorResponse("File must be a YAML file (.yaml or .yml)"));
            }

            String yamlContent = new String(file.getBytes(), StandardCharsets.UTF_8);

            Job job = flinkJobService.registerJobFromYaml(yamlContent);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Job registered successfully");
            response.put("jobId", job.getId());
            response.put("sourceTable", job.getSource().getSourceTable());
            response.put("sinkTable", job.getSink().getSinkTable());

            if (job.getWarnings() != null && !job.getWarnings().isEmpty()) {
                response.put("warnings", job.getWarnings());
            }

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            log.error("Error processing YAML file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to process YAML file: " + e.getMessage()));
        }
    }

    /**
     * Delete a job by ID
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteJob(@PathVariable Integer id) {
        try {
            if (!flinkJobService.existsById(id)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(createErrorResponse("Job not found with ID: " + id));
            }

            flinkJobService.deleteJob(id);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Job deleted successfully");
            response.put("jobId", id);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error deleting job with ID: {}", id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to delete job: " + e.getMessage()));
        }
    }

    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", message);
        return response;
    }
}