package com.openmrs.integration;

import com.openmrs.model.Job;
import com.openmrs.service.FlinkJobService;
import com.openmrs.service.JobClientRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that:
 * 1. Sets up source MySQL (with binlog) and sink MySQL via Testcontainers
 * 2. Creates OpenMRS-like tables in the source
 * 3. Inserts test data (encounter + obs records)
 * 4. Registers an ETL job that flattens vitals from EAV to columnar
 * 5. Waits for Flink CDC to process the data
 * 6. Verifies the transformed data appears in the sink table
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class EtlJobIntegrationTest {

    @Container
    static MySQLContainer<?> sourceDb = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
        .withDatabaseName("openmrs")
        .withUsername("openmrs")
        .withPassword("openmrs")
        .withCommand(
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON"
        );

    @Container
    static MySQLContainer<?> sinkDb = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
        .withDatabaseName("flattened")
        .withUsername("sink_user")
        .withPassword("sink_pass");

    @Autowired
    private FlinkJobService flinkJobService;

    @Autowired
    private JobClientRegistry jobClientRegistry;

    private Job registeredJob;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // App metadata DB stays as H2 (from application-test.properties)
        // Source and sink DBs are provided by testcontainers via YAML at test time
    }

    @BeforeAll
    static void initSourceSchema() throws SQLException {
        // Grant replication privileges needed for CDC (must use root connection)
        try (Connection rootConn = DriverManager.getConnection(
                sourceDb.getJdbcUrl(), "root", sourceDb.getPassword());
             Statement rootStmt = rootConn.createStatement()) {
            rootStmt.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'openmrs'@'%'");
            rootStmt.execute("FLUSH PRIVILEGES");
        }

        try (Connection conn = DriverManager.getConnection(
                sourceDb.getJdbcUrl(), sourceDb.getUsername(), sourceDb.getPassword());
             Statement stmt = conn.createStatement()) {

            // Create minimal OpenMRS encounter table
            stmt.execute("""
                CREATE TABLE encounter (
                    encounter_id INT PRIMARY KEY AUTO_INCREMENT,
                    patient_id INT NOT NULL,
                    encounter_type INT NOT NULL,
                    encounter_datetime DATETIME NOT NULL,
                    location_id INT,
                    visit_id INT,
                    creator INT,
                    date_created DATETIME DEFAULT CURRENT_TIMESTAMP,
                    voided TINYINT(1) DEFAULT 0,
                    uuid VARCHAR(38) NOT NULL
                )
                """);

            // Create minimal obs table
            stmt.execute("""
                CREATE TABLE obs (
                    obs_id INT PRIMARY KEY AUTO_INCREMENT,
                    encounter_id INT NOT NULL,
                    concept_id INT NOT NULL,
                    value_numeric DOUBLE,
                    value_text VARCHAR(255),
                    value_datetime DATETIME,
                    value_coded INT,
                    voided TINYINT(1) DEFAULT 0,
                    FOREIGN KEY (encounter_id) REFERENCES encounter(encounter_id)
                )
                """);
        }
    }

    @AfterEach
    void cleanup() {
        if (registeredJob != null && registeredJob.getId() != null) {
            try {
                flinkJobService.deleteJob(registeredJob.getId());
            } catch (Exception e) {
                // Job may already be stopped
            }
        }
    }

    @Test
    void registerJob_insertSourceData_verifySinkTransformation() throws Exception {
        // 1. Insert source data BEFORE job registration
        insertTestData();

        // 2. Build YAML config pointing to testcontainer URLs
        String yaml = buildTestYaml();

        // 3. Register the ETL job (uses initial scan mode to read existing data)
        registeredJob = flinkJobService.registerJobFromYaml(yaml);

        assertThat(registeredJob.getId()).isNotNull();
        assertThat(registeredJob.getSource().getSourceTable()).isEqualTo("encounter");
        assertThat(registeredJob.getSink().getSinkTable()).isEqualTo("test_vitals");

        // 4. Wait for Flink CDC to process the snapshot and write to sink
        waitForSinkData("test_vitals", 1, Duration.ofSeconds(60));

        // 5. Verify the transformed data in the sink table
        try (Connection conn = DriverManager.getConnection(
                sinkDb.getJdbcUrl(), sinkDb.getUsername(), sinkDb.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_vitals WHERE encounter_id = 1")) {

            assertThat(rs.next()).as("Expected one row in sink for encounter_id=1").isTrue();

            assertThat(rs.getInt("encounter_id")).isEqualTo(1);
            assertThat(rs.getInt("patient_id")).isEqualTo(100);
            assertThat(rs.getDouble("systolic_bp")).isEqualTo(120.0);
            assertThat(rs.getDouble("diastolic_bp")).isEqualTo(80.0);
            assertThat(rs.getDouble("temperature_c")).isEqualTo(36.6);
        }
    }

    @Test
    void registerJob_insertAfterJobStart_cdcPicksUpChange() throws Exception {
        // 1. Register job with empty source (latest-offset won't see old data, but initial will snapshot)
        // We use initial mode, so first insert some base data
        insertTestData();

        String yaml = buildTestYaml();
        registeredJob = flinkJobService.registerJobFromYaml(yaml);

        // Wait for initial snapshot to complete
        waitForSinkData("test_vitals", 1, Duration.ofSeconds(60));

        // 2. Now insert NEW data after the job is running — CDC should pick this up
        try (Connection conn = DriverManager.getConnection(
                sourceDb.getJdbcUrl(), sourceDb.getUsername(), sourceDb.getPassword());
             Statement stmt = conn.createStatement()) {

            stmt.execute("""
                INSERT INTO encounter (encounter_id, patient_id, encounter_type, encounter_datetime, voided, uuid)
                VALUES (2, 200, 5, '2025-02-01 10:00:00', 0, 'uuid-encounter-002')
                """);

            stmt.execute("INSERT INTO obs (encounter_id, concept_id, value_numeric, voided) VALUES (2, 1001, 130.0, 0)");
            stmt.execute("INSERT INTO obs (encounter_id, concept_id, value_numeric, voided) VALUES (2, 1002, 85.0, 0)");
        }

        // 3. Wait for CDC to pick up the new row
        waitForSinkData("test_vitals", 2, Duration.ofSeconds(60));

        // 4. Verify the new row
        try (Connection conn = DriverManager.getConnection(
                sinkDb.getJdbcUrl(), sinkDb.getUsername(), sinkDb.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_vitals WHERE encounter_id = 2")) {

            assertThat(rs.next()).as("Expected row for encounter_id=2 from CDC").isTrue();
            assertThat(rs.getInt("patient_id")).isEqualTo(200);
            assertThat(rs.getDouble("systolic_bp")).isEqualTo(130.0);
            assertThat(rs.getDouble("diastolic_bp")).isEqualTo(85.0);
        }
    }

    private void insertTestData() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                sourceDb.getJdbcUrl(), sourceDb.getUsername(), sourceDb.getPassword());
             Statement stmt = conn.createStatement()) {

            // Clean up from previous test runs
            stmt.execute("DELETE FROM obs");
            stmt.execute("DELETE FROM encounter");

            // Insert a vitals encounter (encounter_type = 5, not voided)
            stmt.execute("""
                INSERT INTO encounter (encounter_id, patient_id, encounter_type, encounter_datetime,
                                       location_id, visit_id, creator, voided, uuid)
                VALUES (1, 100, 5, '2025-01-15 09:30:00', 1, 10, 1, 0, 'uuid-encounter-001')
                """);

            // Insert obs records for vitals concepts
            // concept 1001 = systolic BP
            stmt.execute("INSERT INTO obs (encounter_id, concept_id, value_numeric, voided) VALUES (1, 1001, 120.0, 0)");
            // concept 1002 = diastolic BP
            stmt.execute("INSERT INTO obs (encounter_id, concept_id, value_numeric, voided) VALUES (1, 1002, 80.0, 0)");
            // concept 1003 = temperature
            stmt.execute("INSERT INTO obs (encounter_id, concept_id, value_numeric, voided) VALUES (1, 1003, 36.6, 0)");
        }
    }

    private String buildTestYaml() {
        return String.format("""
            connection:
              jdbc: "%s"
              username: "%s"
              password: "%s"
              scanStartupMode: "initial"

            sourceTable: "encounter"

            lookupTables:
              - "obs"

            sink:
              jdbc: "%s"
              username: "%s"
              password: "%s"
              table: "test_vitals"
              primaryKey:
                - "encounter_id"
              columns:
                - name: "encounter_id"
                  type: "INT"
                - name: "patient_id"
                  type: "INT"
                - name: "encounter_datetime"
                  type: "TIMESTAMP(3)"
                - name: "systolic_bp"
                  type: "DOUBLE"
                - name: "diastolic_bp"
                  type: "DOUBLE"
                - name: "temperature_c"
                  type: "DOUBLE"

            fieldMappings:
              passthroughFields:
                - "encounter_id"
                - "patient_id"
                - "encounter_datetime"

              conceptMappings:
                - column: "systolic_bp"
                  conceptId: 1001
                  valueType: "value_numeric"
                - column: "diastolic_bp"
                  conceptId: 1002
                  valueType: "value_numeric"
                - column: "temperature_c"
                  conceptId: 1003
                  valueType: "value_numeric"

              filters:
                - "encounter_type = 5"
                - "voided = false"
            """,
            sourceDb.getJdbcUrl(), sourceDb.getUsername(), sourceDb.getPassword(),
            sinkDb.getJdbcUrl(), sinkDb.getUsername(), sinkDb.getPassword()
        );
    }

    /**
     * Polls the sink table until the expected number of rows appear or timeout is reached.
     */
    private void waitForSinkData(String tableName, int expectedRows, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int rowCount = 0;

        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = DriverManager.getConnection(
                    sinkDb.getJdbcUrl(), sinkDb.getUsername(), sinkDb.getPassword());
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {

                if (rs.next()) {
                    rowCount = rs.getInt(1);
                    if (rowCount >= expectedRows) {
                        return;
                    }
                }
            } catch (SQLException e) {
                // Table might not exist yet, keep polling
            }

            Thread.sleep(1000);
        }

        throw new AssertionError(
            "Timed out waiting for " + expectedRows + " row(s) in " + tableName +
            ". Found " + rowCount + " after " + timeout.toSeconds() + "s"
        );
    }
}