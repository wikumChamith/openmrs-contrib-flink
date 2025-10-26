package com.openmrs.repository;

import com.openmrs.model.Job;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<Job, Integer> {

    /**
     * Find all jobs ordered by ID descending
     */
    List<Job> findAllByOrderByIdDesc();

    /**
     * Find jobs by source table name
     */
    List<Job> findBySource_SourceTable(String sourceTable);

    /**
     * Find jobs by sink table name
     */
    List<Job> findBySink_SinkTable(String sinkTable);
}