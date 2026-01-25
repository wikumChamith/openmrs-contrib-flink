package com.openmrs.model;

import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
@Entity
@Table(name = "job")
public class Job {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @EqualsAndHashCode.Include
    @ToString.Include
    @Column(name = "id")
    private Integer id;

    @Embedded
    private SourceInfo source;

    @Embedded
    private SinkInfo sink;

    @Column(name = "query_sql", columnDefinition = "TEXT")
    @ToString.Include
    private String sql;

    @Embedded
    private FieldMappings fieldMappings;

    @Column(name = "flink_job_id")
    private String flinkJobId;
}
