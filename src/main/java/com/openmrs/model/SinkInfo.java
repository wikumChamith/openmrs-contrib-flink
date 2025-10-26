package com.openmrs.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class SinkInfo {
    @Column(name = "sink_jdbc")
    private String sinkJdbc;

    @Column(name = "sink_username")
    private String sinkUsername;

    @Column(name = "sink_password")
    private String sinkPassword;

    @Column(name = "sink_table")
    private String sinkTable;

    @ElementCollection
    @CollectionTable(name = "sink_primary_key", joinColumns = @JoinColumn(name = "job_id"))
    @Column(name = "column_name")
    private List<String> sinkPrimaryKey;

    @ElementCollection
    @CollectionTable(name = "sink_column", joinColumns = @JoinColumn(name = "job_id"))
    private List<TableColumn> sinkColumns;
}
