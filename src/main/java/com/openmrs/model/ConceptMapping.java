package com.openmrs.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a mapping from an EAV concept to a flattened column.
 * Used to automatically generate CASE WHEN pivot SQL for obs table.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class ConceptMapping {
    /**
     * Target column name in the flattened table
     */
    @Column(name = "column_name")
    private String column;

    /**
     * OpenMRS concept ID to filter obs records
     */
    @Column(name = "concept_id")
    private Integer conceptId;

    /**
     * OpenMRS concept UUID — resolved to conceptId at job registration time.
     * Users may provide either conceptId or conceptUuid, but not both.
     */
    @Column(name = "concept_uuid")
    private String conceptUuid;

    /**
     * Which value column to use from obs table
     * (e.g., "value_numeric", "value_text", "value_datetime", "value_coded")
     */
    @Column(name = "value_type")
    private String valueType;
}
