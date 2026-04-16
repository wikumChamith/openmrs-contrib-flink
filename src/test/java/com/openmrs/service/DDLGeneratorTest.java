package com.openmrs.service;

import com.openmrs.model.SinkInfo;
import com.openmrs.model.TableColumn;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DDLGeneratorTest {

    private DDLGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DDLGenerator();
    }

    // --- generateSinkTableDDL tests (no DB connection needed) ---

    @Test
    void generateSinkTableDDL_basicColumns() {
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_vitals");
        sink.setSinkJdbc("jdbc:mysql://localhost:3307/flat");
        sink.setSinkUsername("user");
        sink.setSinkPassword("pass");
        sink.setSinkColumns(List.of(
            new TableColumn("encounter_id", "INT"),
            new TableColumn("systolic", "DOUBLE"),
            new TableColumn("patient_name", "STRING")
        ));
        sink.setSinkPrimaryKey(List.of("encounter_id"));

        String ddl = generator.generateSinkTableDDL(sink);

        assertThat(ddl).contains("CREATE TABLE flat_vitals_sink (");
        assertThat(ddl).contains("encounter_id INT");
        assertThat(ddl).contains("systolic DOUBLE");
        assertThat(ddl).contains("patient_name STRING");
        assertThat(ddl).contains("PRIMARY KEY (encounter_id) NOT ENFORCED");
        assertThat(ddl).contains("'connector' = 'jdbc'");
        assertThat(ddl).contains("'url' = 'jdbc:mysql://localhost:3307/flat'");
        assertThat(ddl).contains("'table-name' = 'flat_vitals'");
        assertThat(ddl).contains("'driver' = 'com.mysql.cj.jdbc.Driver'");
        assertThat(ddl).contains("'username' = 'user'");
        assertThat(ddl).contains("'password' = 'pass'");
    }

    @Test
    void generateSinkTableDDL_noPrimaryKey() {
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_data");
        sink.setSinkJdbc("jdbc:mysql://localhost:3307/flat");
        sink.setSinkUsername("user");
        sink.setSinkPassword("pass");
        sink.setSinkColumns(List.of(new TableColumn("col1", "STRING")));
        sink.setSinkPrimaryKey(null);

        String ddl = generator.generateSinkTableDDL(sink);

        assertThat(ddl).doesNotContain("PRIMARY KEY");
    }

    @Test
    void generateSinkTableDDL_compositePrimaryKey() {
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_data");
        sink.setSinkJdbc("jdbc:mysql://localhost:3307/flat");
        sink.setSinkUsername("user");
        sink.setSinkPassword("pass");
        sink.setSinkColumns(List.of(
            new TableColumn("encounter_id", "INT"),
            new TableColumn("obs_id", "INT")
        ));
        sink.setSinkPrimaryKey(List.of("encounter_id", "obs_id"));

        String ddl = generator.generateSinkTableDDL(sink);

        assertThat(ddl).contains("PRIMARY KEY (encounter_id, obs_id) NOT ENFORCED");
    }

    @Test
    void generateSinkTableDDL_emptyPrimaryKeyList() {
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_data");
        sink.setSinkJdbc("jdbc:mysql://localhost:3307/flat");
        sink.setSinkUsername("user");
        sink.setSinkPassword("pass");
        sink.setSinkColumns(List.of(new TableColumn("col1", "STRING")));
        sink.setSinkPrimaryKey(List.of());

        String ddl = generator.generateSinkTableDDL(sink);

        assertThat(ddl).doesNotContain("PRIMARY KEY");
    }

    @Test
    void generateSinkTableDDL_multipleColumns_commaDelimited() {
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("test");
        sink.setSinkJdbc("jdbc:mysql://localhost:3307/flat");
        sink.setSinkUsername("u");
        sink.setSinkPassword("p");
        sink.setSinkColumns(List.of(
            new TableColumn("a", "INT"),
            new TableColumn("b", "STRING"),
            new TableColumn("c", "DOUBLE")
        ));
        sink.setSinkPrimaryKey(null);

        String ddl = generator.generateSinkTableDDL(sink);

        assertThat(ddl).contains("a INT,\n");
        assertThat(ddl).contains("b STRING,\n");
        assertThat(ddl).contains("c DOUBLE\n");
    }
}