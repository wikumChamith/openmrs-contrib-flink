package com.openmrs.service;

import com.openmrs.model.*;
import com.openmrs.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class FlinkJobService {

    private final JobRepository jobRepository;

    private final DDLGenerator ddlGenerator;

    private final FieldMappingSqlGenerator fieldMappingSqlGenerator;

    /**
     * Registers a Flink job from YAML content
     * Flow: Parse YAML → Extract DB metadata → Register Flink job → Save to DB
     */
    public Job registerJobFromYaml(String yamlContent) throws Exception {
        log.info("Starting job registration from YAML");

        Job job = parseYaml(yamlContent);
        log.info("Parsed YAML successfully for table: {}", job.getSource().getSourceTable());

        String sinkTable = job.getSink().getSinkTable();
        List<Job> existingJobs = jobRepository.findBySink_SinkTable(sinkTable);
        if (!existingJobs.isEmpty()) {
            String errorMsg = String.format(
                "Sink table '%s' is already in use by job ID: %d. Each sink table can only be used by one job.",
                sinkTable, existingJobs.get(0).getId()
            );
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        log.info("Validation passed: sink table '{}' is available", sinkTable);

        log.info("Generating DDL for source table: {}", job.getSource().getSourceTable());
        String sourceDDL = ddlGenerator.generateSourceTableDDL(job.getSource());
        log.debug("Source DDL:\n{}", sourceDDL);

        List<String> lookupDDLs = new ArrayList<>();
        if (job.getSource().getSourceLookupTables() != null) {
            for (String lookupTable : job.getSource().getSourceLookupTables()) {
                log.info("Generating DDL for lookup table: {}", lookupTable);
                String lookupDDL = ddlGenerator.generateLookupTableDDL(job.getSource(), lookupTable);
                lookupDDLs.add(lookupDDL);
                log.debug("Lookup DDL for {}:\n{}", lookupTable, lookupDDL);
            }
        }

        log.info("Creating physical sink table: {}", job.getSink().getSinkTable());
        ddlGenerator.createPhysicalSinkTable(job.getSink());

        log.info("Generating DDL for sink table: {}", job.getSink().getSinkTable());
        String sinkDDL = ddlGenerator.generateSinkTableDDL(job.getSink());
        log.debug("Sink DDL:\n{}", sinkDDL);

        log.info("Registering Flink job");
        registerFlinkJob(job, sourceDDL, lookupDDLs, sinkDDL);

        log.info("Saving job to database");
        Job savedJob = jobRepository.save(job);
        log.info("Job saved successfully with ID: {}", savedJob.getId());

        return savedJob;
    }

    /**
     * Parses YAML content into a Job object
     */
    private Job parseYaml(String yamlContent) {
        Yaml yaml = new Yaml();
        Map<String, Object> data = yaml.load(yamlContent);

        Job job = new Job();
        SourceInfo sourceInfo = new SourceInfo();
        SinkInfo sinkInfo = new SinkInfo();

        if (data.containsKey("connection")) {
            Map<String, String> connection = (Map<String, String>) data.get("connection");
            sourceInfo.setSourceJdbc(connection.get("jdbc"));
            sourceInfo.setSourceUsername(connection.get("username"));
            sourceInfo.setSourcePassword(connection.get("password"));
        }

        if (data.containsKey("sourceTable")) {
            sourceInfo.setSourceTable((String) data.get("sourceTable"));
        }

        if (data.containsKey("lookupTables")) {
            sourceInfo.setSourceLookupTables((List<String>) data.get("lookupTables"));
        }

        if (data.containsKey("sink")) {
            Map<String, Object> sink = (Map<String, Object>) data.get("sink");
            sinkInfo.setSinkJdbc((String) sink.get("jdbc"));
            sinkInfo.setSinkUsername((String) sink.get("username"));
            sinkInfo.setSinkPassword((String) sink.get("password"));
            sinkInfo.setSinkTable((String) sink.get("table"));

            if (sink.containsKey("primaryKey")) {
                sinkInfo.setSinkPrimaryKey((List<String>) sink.get("primaryKey"));
            }

            if (sink.containsKey("columns")) {
                List<Map<String, Object>> columns = (List<Map<String, Object>>) sink.get("columns");
                List<TableColumn> tableColumns = new ArrayList<>();
                for (Map<String, Object> col : columns) {
                    TableColumn tableColumn = new TableColumn();
                    tableColumn.setName((String) col.get("name"));
                    tableColumn.setType((String) col.get("type"));
                    tableColumns.add(tableColumn);
                }
                sinkInfo.setSinkColumns(tableColumns);
            }
        }

        boolean hasSql = data.containsKey("sql");
        boolean hasFieldMappings = data.containsKey("fieldMappings");

        if (hasSql && hasFieldMappings) {
            throw new IllegalArgumentException(
                "Configuration cannot contain both 'sql' and 'fieldMappings'. " +
                "Please use either manual SQL or field mappings, not both."
            );
        }

        if (!hasSql && !hasFieldMappings) {
            throw new IllegalArgumentException(
                "Configuration must contain either 'sql' or 'fieldMappings'"
            );
        }

        if (hasSql) {
            job.setSql((String) data.get("sql"));
            log.info("Using manual SQL mode");
        } else {
            // Parse field mappings
            Map<String, Object> fieldMappingsData = (Map<String, Object>) data.get("fieldMappings");
            FieldMappings fieldMappings = parseFieldMappings(fieldMappingsData);
            job.setFieldMappings(fieldMappings);
            log.info("Using field mappings mode");
        }

        job.setSource(sourceInfo);
        job.setSink(sinkInfo);

        return job;
    }

    /**
     * Parses field mappings section from YAML
     */
    private FieldMappings parseFieldMappings(Map<String, Object> data) {
        FieldMappings fieldMappings = new FieldMappings();

        if (data.containsKey("passthroughFields")) {
            fieldMappings.setPassthroughFields((List<String>) data.get("passthroughFields"));
        }

        if (data.containsKey("conceptMappings")) {
            List<Map<String, Object>> conceptMappingsData = (List<Map<String, Object>>) data.get("conceptMappings");
            List<ConceptMapping> conceptMappings = new ArrayList<>();
            for (Map<String, Object> mappingData : conceptMappingsData) {
                ConceptMapping mapping = new ConceptMapping();
                mapping.setColumn((String) mappingData.get("column"));
                mapping.setConceptId((Integer) mappingData.get("conceptId"));
                mapping.setValueType((String) mappingData.get("valueType"));
                conceptMappings.add(mapping);
            }
            fieldMappings.setConceptMappings(conceptMappings);
        }

        if (data.containsKey("lookupFields")) {
            List<Map<String, Object>> lookupFieldsData = (List<Map<String, Object>>) data.get("lookupFields");
            List<LookupField> lookupFields = new ArrayList<>();
            for (Map<String, Object> lookupData : lookupFieldsData) {
                LookupField lookup = new LookupField();
                lookup.setColumn((String) lookupData.get("column"));
                lookup.setTable((String) lookupData.get("table"));
                lookup.setField((String) lookupData.get("field"));
                lookup.setJoinField((String) lookupData.get("joinField"));
                // Optional: different field name on lookup table side
                if (lookupData.containsKey("lookupJoinField")) {
                    lookup.setLookupJoinField((String) lookupData.get("lookupJoinField"));
                }
                lookupFields.add(lookup);
            }
            fieldMappings.setLookupFields(lookupFields);
        }

        // Parse filters
        if (data.containsKey("filters")) {
            fieldMappings.setFilters((List<String>) data.get("filters"));
        }

        return fieldMappings;
    }

    /**
     * Registers and executes the Flink job
     */
    private void registerFlinkJob(Job job, String sourceDDL,
                                   List<String> lookupDDLs, String sinkDDL) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        log.info("Created Flink streaming environment");

        log.info("Executing source DDL");
        tEnv.executeSql(sourceDDL);

        for (int i = 0; i < lookupDDLs.size(); i++) {
            log.info("Executing lookup DDL {}/{}", i + 1, lookupDDLs.size());
            tEnv.executeSql(lookupDDLs.get(i));
        }

        log.info("Executing sink DDL");
        tEnv.executeSql(sinkDDL);

        String transformationSql;
        String insertSQL;
        String sinkTableName = job.getSink().getSinkTable() + "_sink";

        if (job.getSql() != null) {
            transformationSql = job.getSql();
            log.info("Using manual SQL");
            insertSQL = "INSERT INTO " + sinkTableName + " " + transformationSql;
        } else if (job.getFieldMappings() != null) {
            fieldMappingSqlGenerator.validate(job);
            transformationSql = fieldMappingSqlGenerator.generateSql(job);
            log.info("Generated SQL from field mappings");
            log.debug("Generated transformation SQL:\n{}", transformationSql);

            StringBuilder columnList = new StringBuilder("(");
            List<TableColumn> sinkColumns = job.getSink().getSinkColumns();
            for (int i = 0; i < sinkColumns.size(); i++) {
                if (i > 0) columnList.append(", ");
                columnList.append(sinkColumns.get(i).getName());
            }
            columnList.append(")");

            insertSQL = "INSERT INTO " + sinkTableName + " " + columnList + " " + transformationSql;
        } else {
            throw new IllegalStateException("Job must have either sql or fieldMappings");
        }

        log.info("Executing transformation SQL");
        log.debug("Insert SQL:\n{}", insertSQL);
        tEnv.executeSql(insertSQL);

        log.info("Flink job registered and started successfully");
    }
}