package com.openmrs.controller;

import com.openmrs.model.Job;
import com.openmrs.repository.JobRepository;
import com.openmrs.service.FlinkJobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
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

    private final JobRepository jobRepository;

    /**
     * Get all jobs
     *
     * GET /api/jobs
     */
    @GetMapping
    @Transactional
    public ResponseEntity<List<Job>> getAllJobs() {
        try {
            List<Job> jobs = jobRepository.findAll();
            return ResponseEntity.ok(jobs);
        } catch (Exception e) {
            log.error("Error retrieving jobs", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Upload YAML file to register a new Flink CDC ETL job
     *
     * POST /api/jobs/upload
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

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            log.error("Error processing YAML file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to process YAML file: " + e.getMessage()));
        }
    }

    /**
     * Helper method to create error response
     */
    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", message);
        return response;
    }
}