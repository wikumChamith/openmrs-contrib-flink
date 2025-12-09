package com.openmrs.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Container for all field mapping configurations.
 * Provides an alternative to manual SQL by allowing users to define
 * field mappings declaratively in YAML.
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class FieldMappings {
    /**
     * Direct passthrough fields from the source table.
     * These are selected as-is without transformation.
     */
    @ElementCollection
    @CollectionTable(name = "field_mapping_passthrough", joinColumns = @JoinColumn(name = "job_id"))
    @Column(name = "field_name")
    private List<String> passthroughFields;

    /**
     * EAV concept mappings from obs table.
     * Each mapping generates a CASE WHEN pivot clause.
     */
    @ElementCollection
    @CollectionTable(name = "field_mapping_concept", joinColumns = @JoinColumn(name = "job_id"))
    private List<ConceptMapping> conceptMappings;

    /**
     * Simple 1:1 lookup table field mappings.
     * Each mapping generates a LEFT JOIN and field selection.
     */
    @ElementCollection
    @CollectionTable(name = "field_mapping_lookup", joinColumns = @JoinColumn(name = "job_id"))
    private List<LookupField> lookupFields;

    /**
     * WHERE clause filters to apply to the source table.
     * Each string is a complete filter condition (e.g., "encounter_type = 5")
     */
    @ElementCollection
    @CollectionTable(name = "field_mapping_filter", joinColumns = @JoinColumn(name = "job_id"))
    @Column(name = "filter_condition")
    private List<String> filters;
}
