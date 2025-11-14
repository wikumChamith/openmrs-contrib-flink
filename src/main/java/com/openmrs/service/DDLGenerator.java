package com.openmrs.service;

import com.openmrs.dto.ColumnInfo;
import com.openmrs.model.SinkInfo;
import com.openmrs.model.SourceInfo;
import com.openmrs.model.TableColumn;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DDLGenerator {

    /**
     * Generates Flink DDL for the source table using MySQL CDC connector
     */
    public String generateSourceTableDDL(SourceInfo source) throws SQLException {
        String jdbcUrl = source.getSourceJdbc();
        String username = source.getSourceUsername();
        String password = source.getSourcePassword();
        String tableName = source.getSourceTable();
        String databaseName = extractDatabaseFromJdbc(jdbcUrl);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            List<ColumnInfo> columns = getTableColumns(conn, databaseName, tableName);
            String primaryKeyColumn = getPrimaryKeyColumn(conn, databaseName, tableName);

            return buildSourceDDL(tableName, columns, primaryKeyColumn, jdbcUrl, username, password);
        }
    }

    /**
     * Generates Flink DDL for a lookup table using MySQL CDC connector
     */
    public String generateLookupTableDDL(SourceInfo source, String lookupTableName) throws SQLException {
        String jdbcUrl = source.getSourceJdbc();
        String username = source.getSourceUsername();
        String password = source.getSourcePassword();
        String databaseName = extractDatabaseFromJdbc(jdbcUrl);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            List<ColumnInfo> columns = getTableColumns(conn, databaseName, lookupTableName);
            String primaryKeyColumn = getPrimaryKeyColumn(conn, databaseName, lookupTableName);

            return buildLookupDDL(lookupTableName, columns, primaryKeyColumn, jdbcUrl, username, password);
        }
    }

    /**
     * Creates the physical sink table in the target database
     */
    public void createPhysicalSinkTable(SinkInfo sink) throws SQLException {
        String jdbcUrl = sink.getSinkJdbc();
        String username = sink.getSinkUsername();
        String password = sink.getSinkPassword();
        String tableName = sink.getSinkTable();

        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        boolean first = true;
        for (TableColumn column : sink.getSinkColumns()) {
            if (!first) createTableSQL.append(",\n");
            first = false;
            createTableSQL.append("    ").append(column.getName()).append(" ");
            createTableSQL.append(mapFlinkTypeToMysql(column.getType()));
        }

        if (sink.getSinkPrimaryKey() != null && !sink.getSinkPrimaryKey().isEmpty()) {
            createTableSQL.append(",\n    PRIMARY KEY (");
            createTableSQL.append(String.join(", ", sink.getSinkPrimaryKey()));
            createTableSQL.append(")");
        }

        createTableSQL.append("\n)");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSQL.toString());
            System.out.println("Physical sink table created successfully: " + tableName);
        }
    }

    /**
     * Maps Flink data types to MySQL data types for physical table creation
     * Only handles types that need conversion, others pass through as-is
     */
    private String mapFlinkTypeToMysql(String flinkType) {
        String upperType = flinkType.toUpperCase();

        return switch (upperType) {
            case "BOOLEAN" -> "TINYINT(1)";
            case "STRING" -> "VARCHAR(255)";
            case "BYTES" -> "BLOB";
            default -> {
                if (upperType.startsWith("TIMESTAMP")) yield "TIMESTAMP";
                yield flinkType;
            }
        };
    }

    /**
     * Generates Flink DDL for the sink table using JDBC connector
     */
    public String generateSinkTableDDL(SinkInfo sink) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(sink.getSinkTable()).append("_sink (\n");

        boolean first = true;
        for (TableColumn column : sink.getSinkColumns()) {
            if (!first) ddl.append(",\n");
            first = false;
            ddl.append("    ").append(column.getName()).append(" ").append(column.getType());
        }

        if (sink.getSinkPrimaryKey() != null && !sink.getSinkPrimaryKey().isEmpty()) {
            ddl.append(",\n    PRIMARY KEY (");
            ddl.append(String.join(", ", sink.getSinkPrimaryKey()));
            ddl.append(") NOT ENFORCED");
        }

        ddl.append("\n) WITH (\n");
        ddl.append("    'connector' = 'jdbc',\n");
        ddl.append("    'url' = '").append(sink.getSinkJdbc()).append("',\n");
        ddl.append("    'table-name' = '").append(sink.getSinkTable()).append("',\n");
        ddl.append("    'driver' = 'com.mysql.cj.jdbc.Driver',\n");
        ddl.append("    'username' = '").append(sink.getSinkUsername()).append("',\n");
        ddl.append("    'password' = '").append(sink.getSinkPassword()).append("'\n");
        ddl.append(")");

        System.out.println("=== GENERATED SINK DDL ===");
        System.out.println(ddl);
        System.out.println("=========================");

        return ddl.toString();
    }

    /**
     * Retrieves column metadata from database
     */
    private List<ColumnInfo> getTableColumns(Connection conn, String databaseName, String tableName) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();

        Set<String> primaryKeyColumns = new HashSet<>();
        try (ResultSet pkRs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            while (pkRs.next()) {
                primaryKeyColumns.add(pkRs.getString("COLUMN_NAME"));
            }
        }

        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("TYPE_NAME");
                Integer columnSize = rs.getInt("COLUMN_SIZE");
                Integer decimalDigits = rs.getInt("DECIMAL_DIGITS");
                String nullable = rs.getString("IS_NULLABLE");
                boolean isPrimaryKey = primaryKeyColumns.contains(columnName);

                columns.add(new ColumnInfo(
                        columnName,
                        dataType,
                        columnSize,
                        decimalDigits,
                        "YES".equalsIgnoreCase(nullable),
                        isPrimaryKey
                ));
            }
        }

        return columns;
    }

    /**
     * Gets the first primary key column (for incremental snapshot)
     */
    private String getPrimaryKeyColumn(Connection conn, String databaseName, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
        }
        return null;
    }

    /**
     * Builds the source table DDL with MySQL CDC connector
     */
    private String buildSourceDDL(String tableName, List<ColumnInfo> columns,
                                   String primaryKeyColumn, String jdbcUrl,
                                   String username, String password) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append("_source (\n");

        boolean first = true;
        for (ColumnInfo column : columns) {
            if (!first) ddl.append(",\n");
            first = false;

            String flinkType = mapToFlinkType(column.getDataType(), column.getColumnSize(), column.getDecimalDigits());
            ddl.append("    ").append(column.getColumnName()).append(" ").append(flinkType);
        }

        ddl.append("\n) WITH (\n");
        ddl.append("    'connector' = 'mysql-cdc',\n");
        ddl.append("    'hostname' = '").append(extractHostFromJdbc(jdbcUrl)).append("',\n");
        ddl.append("    'port' = '").append(extractPortFromJdbc(jdbcUrl)).append("',\n");
        ddl.append("    'database-name' = '").append(extractDatabaseFromJdbc(jdbcUrl)).append("',\n");
        ddl.append("    'table-name' = '").append(tableName).append("',\n");
        ddl.append("    'username' = '").append(username).append("',\n");
        ddl.append("    'scan.startup.mode' = '").append("latest-offset").append("',\n");
        ddl.append("    'password' = '").append(password).append("',\n");
        ddl.append("    'server-time-zone' = 'UTC'");

        if (primaryKeyColumn != null) {
            ddl.append(",\n    'scan.incremental.snapshot.chunk.key-column' = '").append(primaryKeyColumn).append("'");
        }

        ddl.append("\n)");

        System.out.println("=== GENERATED SOURCE DDL ===");
        System.out.println(ddl);
        System.out.println("============================");

        return ddl.toString();
    }

    /**
     * Builds the lookup table DDL with MySQL CDC connector
     */
    private String buildLookupDDL(String tableName, List<ColumnInfo> columns,
                                   String primaryKeyColumn, String jdbcUrl,
                                   String username, String password) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE lkp_").append(tableName).append(" (\n");

        boolean first = true;
        for (ColumnInfo column : columns) {
            if (!first) ddl.append(",\n");
            first = false;

            String flinkType = mapToFlinkType(column.getDataType(), column.getColumnSize(), column.getDecimalDigits());
            ddl.append("    ").append(column.getColumnName()).append(" ").append(flinkType);
        }

        ddl.append("\n) WITH (\n");
        ddl.append("    'connector' = 'mysql-cdc',\n");
        ddl.append("    'hostname' = '").append(extractHostFromJdbc(jdbcUrl)).append("',\n");
        ddl.append("    'port' = '").append(extractPortFromJdbc(jdbcUrl)).append("',\n");
        ddl.append("    'database-name' = '").append(extractDatabaseFromJdbc(jdbcUrl)).append("',\n");
        ddl.append("    'table-name' = '").append(tableName).append("',\n");
        ddl.append("    'username' = '").append(username).append("',\n");
        ddl.append("    'password' = '").append(password).append("',\n");
        ddl.append("    'server-time-zone' = 'UTC'");

        if (primaryKeyColumn != null) {
            ddl.append(",\n    'scan.incremental.snapshot.chunk.key-column' = '").append(primaryKeyColumn).append("'");
        }

        ddl.append("\n)");

        return ddl.toString();
    }

    /**
     * Maps MySQL data types to Flink SQL data types
     * Only handles types that need conversion, numeric types pass through
     */
    private String mapToFlinkType(String mysqlType, Integer columnSize, Integer decimalDigits) {
        String upperType = mysqlType.toUpperCase();

        return switch (upperType) {
            case "TINYINT", "BIT" -> "BOOLEAN";
            case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "JSON" -> "STRING";
            case "DATETIME", "TIMESTAMP" -> "TIMESTAMP(3)";
            case "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB" -> "BYTES";
            case "DECIMAL", "NUMERIC" -> {
                if (columnSize != null && decimalDigits != null) {
                    yield "DECIMAL(" + columnSize + ", " + decimalDigits + ")";
                }
                yield "DECIMAL";
            }
            default -> upperType;
        };
    }

    /**
     * Extracts hostname from JDBC URL
     */
    private String extractHostFromJdbc(String jdbcUrl) {
        Pattern pattern = Pattern.compile("jdbc:mysql://([^:/]+)");
        Matcher matcher = pattern.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "localhost";
    }

    /**
     * Extracts port from JDBC URL
     */
    private String extractPortFromJdbc(String jdbcUrl) {
        Pattern pattern = Pattern.compile("jdbc:mysql://[^:]+:(\\d+)");
        Matcher matcher = pattern.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "3306";
    }

    /**
     * Extracts database name from JDBC URL
     */
    private String extractDatabaseFromJdbc(String jdbcUrl) {
        Pattern pattern = Pattern.compile("jdbc:mysql://[^/]+/(\\w+)");
        Matcher matcher = pattern.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Cannot extract database name from JDBC URL: " + jdbcUrl);
    }
}