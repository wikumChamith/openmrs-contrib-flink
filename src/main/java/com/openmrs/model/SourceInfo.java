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
public class SourceInfo {
    @Column(name = "source_jdbc")
    private String sourceJdbc;

    @Column(name = "source_username")
    private String sourceUsername;

    @Column(name = "source_password")
    private String sourcePassword;

    @Column(name = "source_table")
    private String sourceTable;

    @ElementCollection
    @CollectionTable(name = "source_lookup_table", joinColumns = @JoinColumn(name = "job_id"))
    @Column(name = "table_name")
    private List<String> sourceLookupTables;
}
