package com.openmrs.service;

import com.openmrs.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FieldMappingSqlGeneratorTest {

    private FieldMappingSqlGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new FieldMappingSqlGenerator();
    }

    // --- generateSql tests ---

    @Test
    void generateSql_passthroughFieldsOnly() {
        Job job = buildJob(
            List.of("encounter_id", "encounter_datetime"),
            null, null, null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("encounter_datetime", "TIMESTAMP(3)")
            )
        );

        String sql = generator.generateSql(job);

        assertThat(sql).contains("SELECT");
        assertThat(sql).contains("e.encounter_id");
        assertThat(sql).contains("e.encounter_datetime");
        assertThat(sql).contains("FROM encounter_source e");
        // No GROUP BY when no concept mappings
        assertThat(sql).doesNotContain("GROUP BY");
    }

    @Test
    void generateSql_withConceptMappings() {
        Job job = buildJob(
            List.of("encounter_id"),
            List.of(new ConceptMapping("systolic", 5085, null, "value_numeric")),
            null, null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("systolic", "DOUBLE")
            )
        );
        job.getSource().setSourceLookupTables(List.of("obs"));

        String sql = generator.generateSql(job);

        assertThat(sql).contains("MAX(CASE WHEN o.concept_id = 5085 THEN o.value_numeric END) as systolic");
        assertThat(sql).contains("LEFT JOIN lkp_obs o");
        assertThat(sql).contains("ON e.encounter_id = o.encounter_id");
        assertThat(sql).contains("AND o.voided = false");
        assertThat(sql).contains("GROUP BY");
        assertThat(sql).contains("e.encounter_id");
    }

    @Test
    void generateSql_withLookupFields() {
        Job job = buildJob(
            List.of("encounter_id"),
            null,
            List.of(new LookupField("location_name", "location", "name", "location_id", null)),
            null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("location_name", "STRING")
            )
        );
        job.getSource().setSourceLookupTables(List.of("location"));

        String sql = generator.generateSql(job);

        assertThat(sql).contains("location.name as location_name");
        assertThat(sql).contains("LEFT JOIN lkp_location location");
        assertThat(sql).contains("ON e.location_id = location.location_id");
    }

    @Test
    void generateSql_withLookupJoinField() {
        LookupField lookup = new LookupField("given_name", "person_name", "given_name", "patient_id", "person_id");
        Job job = buildJob(
            List.of("encounter_id"),
            null,
            List.of(lookup),
            null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("given_name", "STRING")
            )
        );
        job.getSource().setSourceLookupTables(List.of("person_name"));

        String sql = generator.generateSql(job);

        assertThat(sql).contains("ON e.patient_id = person_name.person_id");
    }

    @Test
    void generateSql_withFilters() {
        Job job = buildJob(
            List.of("encounter_id"),
            null, null,
            List.of("encounter_type = 5", "voided = false"),
            List.of(new TableColumn("encounter_id", "INT"))
        );

        String sql = generator.generateSql(job);

        assertThat(sql).contains("WHERE");
        assertThat(sql).contains("e.encounter_type = 5");
        assertThat(sql).contains("AND e.voided = false");
    }

    @Test
    void generateSql_filterWithDotNotation_noPrefix() {
        Job job = buildJob(
            List.of("encounter_id"),
            null, null,
            List.of("location.retired = false"),
            List.of(new TableColumn("encounter_id", "INT"))
        );

        String sql = generator.generateSql(job);

        // Filters with dots should not get the source alias prefix
        assertThat(sql).contains("location.retired = false");
        assertThat(sql).doesNotContain("e.location.retired");
    }

    @Test
    void generateSql_sinkColumnOrderIsRespected() {
        Job job = buildJob(
            List.of("encounter_id", "encounter_datetime"),
            List.of(new ConceptMapping("systolic", 5085, null, "value_numeric")),
            null, null,
            List.of(
                new TableColumn("encounter_datetime", "TIMESTAMP(3)"),
                new TableColumn("systolic", "DOUBLE"),
                new TableColumn("encounter_id", "INT")
            )
        );
        job.getSource().setSourceLookupTables(List.of("obs"));

        String sql = generator.generateSql(job);

        int datetimePos = sql.indexOf("e.encounter_datetime");
        int systolicPos = sql.indexOf("MAX(CASE WHEN");
        int idPos = sql.indexOf("e.encounter_id");

        assertThat(datetimePos).isLessThan(systolicPos);
        assertThat(systolicPos).isLessThan(idPos);
    }

    @Test
    void generateSql_nullFieldMappings_throws() {
        Job job = new Job();
        job.setFieldMappings(null);

        assertThatThrownBy(() -> generator.generateSql(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Field mappings cannot be null");
    }

    @Test
    void generateSql_sinkColumnNotInMappings_throws() {
        Job job = buildJob(
            List.of("encounter_id"),
            null, null, null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("unknown_column", "STRING")
            )
        );

        assertThatThrownBy(() -> generator.generateSql(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown_column");
    }

    @Test
    void generateSql_conceptMappingsWithoutObsInLookup_throws() {
        Job job = buildJob(
            List.of("encounter_id"),
            List.of(new ConceptMapping("systolic", 5085, null, "value_numeric")),
            null, null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("systolic", "DOUBLE")
            )
        );
        // obs NOT in lookup tables
        job.getSource().setSourceLookupTables(List.of("location"));

        assertThatThrownBy(() -> generator.generateSql(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("obs");
    }

    @Test
    void generateSql_lookupFieldTableNotInLookupTables_throws() {
        Job job = buildJob(
            List.of("encounter_id"),
            null,
            List.of(new LookupField("loc_name", "location", "name", "location_id", null)),
            null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("loc_name", "STRING")
            )
        );
        // location NOT in lookup tables
        job.getSource().setSourceLookupTables(List.of("obs"));

        assertThatThrownBy(() -> generator.generateSql(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("location");
    }

    @Test
    void generateSql_multipleLookupsOnSameTable_singleJoin() {
        Job job = buildJob(
            List.of("encounter_id"),
            null,
            List.of(
                new LookupField("given_name", "person_name", "given_name", "patient_id", "person_id"),
                new LookupField("family_name", "person_name", "family_name", "patient_id", "person_id")
            ),
            null,
            List.of(
                new TableColumn("encounter_id", "INT"),
                new TableColumn("given_name", "STRING"),
                new TableColumn("family_name", "STRING")
            )
        );
        job.getSource().setSourceLookupTables(List.of("person_name"));

        String sql = generator.generateSql(job);

        // Only one LEFT JOIN for person_name
        int firstJoin = sql.indexOf("LEFT JOIN lkp_person_name");
        int secondJoin = sql.indexOf("LEFT JOIN lkp_person_name", firstJoin + 1);
        assertThat(firstJoin).isGreaterThan(-1);
        assertThat(secondJoin).isEqualTo(-1);
    }

    // --- validate tests ---

    @Test
    void validate_validPassthroughOnly() {
        Job job = buildJob(List.of("id"), null, null, null, null);
        generator.validate(job);
    }

    @Test
    void validate_validConceptMappings() {
        Job job = buildJob(
            null,
            List.of(new ConceptMapping("systolic", 5085, null, "value_numeric")),
            null, null, null
        );
        generator.validate(job);
    }

    @Test
    void validate_nullMappings_throws() {
        Job job = new Job();
        job.setFieldMappings(null);

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be null");
    }

    @Test
    void validate_emptyMappings_throws() {
        Job job = buildJob(null, null, null, null, null);

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one of");
    }

    @Test
    void validate_invalidValueType_throws() {
        Job job = buildJob(
            null,
            List.of(new ConceptMapping("col", 1, null, "invalid_type")),
            null, null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid valueType");
    }

    @Test
    void validate_conceptMappingNullConceptId_throws() {
        Job job = buildJob(
            null,
            List.of(new ConceptMapping("col", null, null, "value_numeric")),
            null, null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("no resolved conceptId");
    }

    @Test
    void validate_emptyConceptColumn_throws() {
        Job job = buildJob(
            null,
            List.of(new ConceptMapping("", 1, null, "value_numeric")),
            null, null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column name cannot be empty");
    }

    @Test
    void validate_lookupMissingTable_throws() {
        Job job = buildJob(
            null, null,
            List.of(new LookupField("col", null, "field", "join", null)),
            null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("table cannot be empty");
    }

    @Test
    void validate_lookupMissingField_throws() {
        Job job = buildJob(
            null, null,
            List.of(new LookupField("col", "table", null, "join", null)),
            null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("field cannot be empty");
    }

    @Test
    void validate_lookupMissingJoinField_throws() {
        Job job = buildJob(
            null, null,
            List.of(new LookupField("col", "table", "field", null, null)),
            null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("joinField cannot be empty");
    }

    @Test
    void validate_duplicateColumnNames_throws() {
        Job job = buildJob(
            List.of("id"),
            List.of(new ConceptMapping("id", 1, null, "value_numeric")),
            null, null, null
        );

        assertThatThrownBy(() -> generator.validate(job))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate column name");
    }

    @Test
    void validate_allValueTypes_accepted() {
        for (String type : List.of("value_numeric", "value_text", "value_datetime", "value_coded")) {
            Job job = buildJob(
                null,
                List.of(new ConceptMapping("col", 1, null, type)),
                null, null, null
            );
            generator.validate(job);
        }
    }

    // --- Helper ---

    private Job buildJob(List<String> passthrough, List<ConceptMapping> concepts,
                         List<LookupField> lookups, List<String> filters,
                         List<TableColumn> sinkColumns) {
        Job job = new Job();

        SourceInfo source = new SourceInfo();
        source.setSourceTable("encounter");
        source.setSourceJdbc("jdbc:mysql://localhost:3306/openmrs");
        source.setSourceUsername("user");
        source.setSourcePassword("pass");
        source.setScanStartupMode("latest-offset");
        job.setSource(source);

        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_encounter");
        sink.setSinkJdbc("jdbc:mysql://localhost:3307/flat");
        sink.setSinkUsername("user");
        sink.setSinkPassword("pass");
        sink.setSinkColumns(sinkColumns);
        sink.setSinkPrimaryKey(List.of("encounter_id"));
        job.setSink(sink);

        FieldMappings mappings = new FieldMappings();
        mappings.setPassthroughFields(passthrough);
        mappings.setConceptMappings(concepts);
        mappings.setLookupFields(lookups);
        mappings.setFilters(filters);
        job.setFieldMappings(mappings);

        return job;
    }
}