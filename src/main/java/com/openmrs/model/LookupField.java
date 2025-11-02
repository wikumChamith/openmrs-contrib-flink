package com.openmrs.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a simple 1:1 field mapping from a lookup table.
 * Automatically generates LEFT JOIN with standard join pattern:
 * e.{joinField} = {table}.{joinField}
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class LookupField {
    /**
     * Target column name in the flattened table
     */
    @Column(name = "column_name")
    private String column;

    /**
     * Lookup table name (will be prefixed with "lkp_" in generated SQL)
     */
    @Column(name = "table_name")
    private String table;

    /**
     * Field to select from the lookup table
     */
    @Column(name = "field_name")
    private String field;

    /**
     * Field to join on from the source table side
     * e.g., "patient_id" in e.patient_id
     */
    @Column(name = "join_field")
    private String joinField;

    /**
     * Field to join on from the lookup table side (optional)
     * If not specified, uses joinField for both sides
     * e.g., "person_id" in person_name.person_id
     * Generates: e.patient_id = person_name.person_id
     */
    @Column(name = "lookup_join_field")
    private String lookupJoinField;
}
