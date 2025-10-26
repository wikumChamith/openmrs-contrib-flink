package com.openmrs.service;

import com.openmrs.model.Job;
import com.openmrs.repository.JobRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class JobStartupRunner implements ApplicationRunner {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DDLGenerator ddlGenerator;

    @Override
    @Transactional
    public void run(ApplicationArguments args) throws Exception {
        log.info("=== Starting Job Startup Runner ===");

        List<Job> jobs = jobRepository.findAll();

        if (jobs.isEmpty()) {
            log.info("No jobs found in database. Skipping job restoration.");
            return;
        }

        log.info("Found {} job(s) in database. Starting restoration...", jobs.size());

        for (Job job : jobs) {
            try {
                log.info("Restoring job ID: {} for source table: {} -> sink table: {}",
                    job.getId(), job.getSource().getSourceTable(), job.getSink().getSinkTable());
                restoreJob(job);
                log.info("Successfully restored job ID: {}", job.getId());
            } catch (Exception e) {
                log.error("Failed to restore job ID: {} - {}", job.getId(), e.getMessage(), e);
            }
        }

        log.info("=== Job Startup Runner completed ===");
    }

    private void restoreJob(Job job) throws Exception {
        log.debug("Generating DDL for source table: {}", job.getSource().getSourceTable());
        String sourceDDL = ddlGenerator.generateSourceTableDDL(job.getSource());

        List<String> lookupDDLs = new ArrayList<>();
        if (job.getSource().getSourceLookupTables() != null) {
            for (String lookupTable : job.getSource().getSourceLookupTables()) {
                log.debug("Generating DDL for lookup table: {}", lookupTable);
                String lookupDDL = ddlGenerator.generateLookupTableDDL(job.getSource(), lookupTable);
                lookupDDLs.add(lookupDDL);
            }
        }

        log.debug("Ensuring physical sink table exists: {}", job.getSink().getSinkTable());
        ddlGenerator.createPhysicalSinkTable(job.getSink());

        log.debug("Generating DDL for sink table: {}", job.getSink().getSinkTable());
        String sinkDDL = ddlGenerator.generateSinkTableDDL(job.getSink());

        log.debug("Registering Flink job for source table: {}", job.getSource().getSourceTable());
        registerFlinkJob(job, sourceDDL, lookupDDLs, sinkDDL);
    }

    private void registerFlinkJob(Job job, String sourceDDL,
                                   List<String> lookupDDLs, String sinkDDL) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(sourceDDL);

        for (String lookupDDL : lookupDDLs) {
            tEnv.executeSql(lookupDDL);
        }

        tEnv.executeSql(sinkDDL);

        String sinkTableName = job.getSink().getSinkTable() + "_sink";
        String insertSQL = "INSERT INTO " + sinkTableName + " " + job.getSql();
        tEnv.executeSql(insertSQL);

        log.info("Flink job started for source: {} -> sink: {}",
            job.getSource().getSourceTable(), job.getSink().getSinkTable());
    }
}