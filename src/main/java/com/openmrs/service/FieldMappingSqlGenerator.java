package com.openmrs.service;

import com.openmrs.model.*;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generates Flink SQL queries from field mapping configurations.
 * Converts declarative YAML field mappings into SQL SELECT statements
 * with automatic EAV pivoting and lookup table joins.
 */
@Service
public class FieldMappingSqlGenerator {

    /**
     * Generates a complete SQL query from field mappings.
     *
     * @param job The job containing field mappings and source/sink info
     * @return SQL SELECT statement with joins, pivots, filters, and grouping
     */
    public String generateSql(Job job) {
        FieldMappings mappings = job.getFieldMappings();
        if (mappings == null) {
            throw new IllegalArgumentException("Field mappings cannot be null");
        }

        SourceInfo source = job.getSource();
        SinkInfo sink = job.getSink();
        String sourceTableAlias = "e";
        String obsTableAlias = "o";

        StringBuilder sql = new StringBuilder();

        sql.append("SELECT\n");
        appendSelectFieldsInSinkOrder(sql, mappings, sink, sourceTableAlias, obsTableAlias);

        sql.append("FROM ").append(source.getSourceTable()).append("_source ").append(sourceTableAlias).append("\n");

        appendJoins(sql, mappings, source, sourceTableAlias, obsTableAlias);

        appendWhereClause(sql, mappings, sourceTableAlias);

        appendGroupBy(sql, mappings, sourceTableAlias);

        return sql.toString();
    }

    /**
     * Appends SELECT field list in the exact order of sink columns.
     * This ensures Flink's schema matching succeeds.
     */
    private void appendSelectFieldsInSinkOrder(StringBuilder sql, FieldMappings mappings, SinkInfo sink,
                                                String sourceTableAlias, String obsTableAlias) {
        List<TableColumn> sinkColumns = sink.getSinkColumns();
        if (sinkColumns == null || sinkColumns.isEmpty()) {
            throw new IllegalArgumentException("Sink columns cannot be empty");
        }

        Set<String> passthroughFields = new HashSet<>();
        if (mappings.getPassthroughFields() != null) {
            passthroughFields.addAll(mappings.getPassthroughFields());
        }

        Map<String, ConceptMapping> conceptMap = new HashMap<>();
        if (mappings.getConceptMappings() != null) {
            for (ConceptMapping cm : mappings.getConceptMappings()) {
                conceptMap.put(cm.getColumn(), cm);
            }
        }

        Map<String, LookupField> lookupMap = new HashMap<>();
        if (mappings.getLookupFields() != null) {
            for (LookupField lf : mappings.getLookupFields()) {
                lookupMap.put(lf.getColumn(), lf);
            }
        }

        boolean first = true;
        for (TableColumn sinkColumn : sinkColumns) {
            String columnName = sinkColumn.getName();

            if (!first) sql.append(",\n");

            if (passthroughFields.contains(columnName)) {
                sql.append("  ").append(sourceTableAlias).append(".").append(columnName);
            } else if (conceptMap.containsKey(columnName)) {
                ConceptMapping mapping = conceptMap.get(columnName);
                sql.append("  MAX(CASE WHEN ")
                   .append(obsTableAlias).append(".concept_id = ")
                   .append(mapping.getConceptId())
                   .append(" THEN ")
                   .append(obsTableAlias).append(".").append(mapping.getValueType())
                   .append(" END) as ")
                   .append(mapping.getColumn());
            } else if (lookupMap.containsKey(columnName)) {
                LookupField lookup = lookupMap.get(columnName);
                sql.append("  ").append(lookup.getTable()).append(".").append(lookup.getField())
                   .append(" as ").append(lookup.getColumn());
            } else {
                throw new IllegalArgumentException(
                    "Sink column '" + columnName + "' not found in any field mappings. " +
                    "Please add it to passthroughFields, conceptMappings, or lookupFields."
                );
            }

            first = false;
        }

        sql.append("\n");
    }

    /**
     * Appends LEFT JOIN clauses for obs table (if concepts exist) and lookup tables
     */
    private void appendJoins(StringBuilder sql, FieldMappings mappings, SourceInfo source,
                            String sourceTableAlias, String obsTableAlias) {
        if (mappings.getConceptMappings() != null && !mappings.getConceptMappings().isEmpty()) {
            boolean obsInLookup = source.getSourceLookupTables() != null &&
                                 source.getSourceLookupTables().contains("obs");

            if (!obsInLookup) {
                throw new IllegalArgumentException(
                    "Field mappings contain concept mappings but 'obs' is not in lookupTables. " +
                    "Please add 'obs' to lookupTables in YAML configuration."
                );
            }

            sql.append("LEFT JOIN lkp_obs ").append(obsTableAlias).append("\n");
            sql.append("  ON ").append(sourceTableAlias).append(".").append(getSourcePrimaryKey(source))
               .append(" = ").append(obsTableAlias).append(".").append(getSourcePrimaryKey(source)).append("\n");
            sql.append("  AND ").append(obsTableAlias).append(".voided = false\n");
        }

        if (mappings.getLookupFields() != null && !mappings.getLookupFields().isEmpty()) {
            Set<String> joinedTables = new HashSet<>();

            for (LookupField lookup : mappings.getLookupFields()) {
                String tableName = lookup.getTable();

                if (joinedTables.contains(tableName)) {
                    continue;
                }

                boolean tableInLookup = source.getSourceLookupTables() != null &&
                                       source.getSourceLookupTables().contains(tableName);

                if (!tableInLookup) {
                    throw new IllegalArgumentException(
                        "Field mappings reference lookup table '" + tableName +
                        "' but it's not in lookupTables. Please add it to lookupTables in YAML configuration."
                    );
                }

                sql.append("LEFT JOIN lkp_").append(tableName).append(" ").append(tableName).append("\n");

                String lookupSideField = (lookup.getLookupJoinField() != null && !lookup.getLookupJoinField().trim().isEmpty())
                    ? lookup.getLookupJoinField()
                    : lookup.getJoinField();

                sql.append("  ON ").append(sourceTableAlias).append(".").append(lookup.getJoinField())
                   .append(" = ").append(tableName).append(".").append(lookupSideField).append("\n");

                joinedTables.add(tableName);
            }
        }
    }

    /**
     * Appends WHERE clause with filters
     */
    private void appendWhereClause(StringBuilder sql, FieldMappings mappings,
                                   String sourceTableAlias) {
        if (mappings.getFilters() != null && !mappings.getFilters().isEmpty()) {
            sql.append("WHERE ");
            boolean first = true;
            for (String filter : mappings.getFilters()) {
                if (!first) sql.append("\n  AND ");
                if (!filter.contains(".")) {
                    sql.append(sourceTableAlias).append(".");
                }
                sql.append(filter);
                first = false;
            }
            sql.append("\n");
        }
    }

    /**
     * Appends GROUP BY clause.
     * Groups by all passthrough fields and lookup fields (but not concept mappings which are aggregated)
     */
    private void appendGroupBy(StringBuilder sql, FieldMappings mappings, String sourceTableAlias) {
        if (mappings.getConceptMappings() == null || mappings.getConceptMappings().isEmpty()) {
            return;
        }

        sql.append("GROUP BY\n");
        boolean first = true;

        if (mappings.getPassthroughFields() != null && !mappings.getPassthroughFields().isEmpty()) {
            for (String field : mappings.getPassthroughFields()) {
                if (!first) sql.append(",\n");
                sql.append("  ").append(sourceTableAlias).append(".").append(field);
                first = false;
            }
        }

        if (mappings.getLookupFields() != null && !mappings.getLookupFields().isEmpty()) {
            for (LookupField lookup : mappings.getLookupFields()) {
                if (!first) sql.append(",\n");
                sql.append("  ").append(lookup.getTable()).append(".").append(lookup.getField());
                first = false;
            }
        }

        sql.append("\n");
    }

    /**
     * Determines the primary key of the source table.
     * Assumes encounter_id for encounter table, otherwise uses {table}_id pattern.
     */
    private String getSourcePrimaryKey(SourceInfo source) {
        String tableName = source.getSourceTable();
        return tableName + "_id";
    }

    /**
     * Validates that field mappings configuration is complete and consistent
     */
    public void validate(Job job) {
        FieldMappings mappings = job.getFieldMappings();
        if (mappings == null) {
            throw new IllegalArgumentException("Field mappings cannot be null");
        }

        boolean hasPassthrough = mappings.getPassthroughFields() != null && !mappings.getPassthroughFields().isEmpty();
        boolean hasConcepts = mappings.getConceptMappings() != null && !mappings.getConceptMappings().isEmpty();
        boolean hasLookups = mappings.getLookupFields() != null && !mappings.getLookupFields().isEmpty();

        if (!hasPassthrough && !hasConcepts && !hasLookups) {
            throw new IllegalArgumentException(
                "Field mappings must contain at least one of: passthroughFields, conceptMappings, or lookupFields"
            );
        }

        if (hasConcepts) {
            for (ConceptMapping mapping : mappings.getConceptMappings()) {
                if (mapping.getColumn() == null || mapping.getColumn().trim().isEmpty()) {
                    throw new IllegalArgumentException("Concept mapping column name cannot be empty");
                }
                if (mapping.getConceptId() == null) {
                    throw new IllegalArgumentException("Concept mapping conceptId cannot be null for column: " + mapping.getColumn());
                }
                if (mapping.getValueType() == null || mapping.getValueType().trim().isEmpty()) {
                    throw new IllegalArgumentException("Concept mapping valueType cannot be empty for column: " + mapping.getColumn());
                }
                // Validate value type is one of the expected obs columns
                String valueType = mapping.getValueType();
                if (!valueType.equals("value_numeric") && !valueType.equals("value_text") &&
                    !valueType.equals("value_datetime") && !valueType.equals("value_coded")) {
                    throw new IllegalArgumentException(
                        "Invalid valueType '" + valueType + "' for column " + mapping.getColumn() +
                        ". Must be one of: value_numeric, value_text, value_datetime, value_coded"
                    );
                }
            }
        }

        if (hasLookups) {
            for (LookupField lookup : mappings.getLookupFields()) {
                if (lookup.getColumn() == null || lookup.getColumn().trim().isEmpty()) {
                    throw new IllegalArgumentException("Lookup field column name cannot be empty");
                }
                if (lookup.getTable() == null || lookup.getTable().trim().isEmpty()) {
                    throw new IllegalArgumentException("Lookup field table cannot be empty for column: " + lookup.getColumn());
                }
                if (lookup.getField() == null || lookup.getField().trim().isEmpty()) {
                    throw new IllegalArgumentException("Lookup field field cannot be empty for column: " + lookup.getColumn());
                }
                if (lookup.getJoinField() == null || lookup.getJoinField().trim().isEmpty()) {
                    throw new IllegalArgumentException("Lookup field joinField cannot be empty for column: " + lookup.getColumn());
                }
            }
        }

        Set<String> columnNames = new HashSet<>();

        if (hasPassthrough) {
            for (String field : mappings.getPassthroughFields()) {
                if (!columnNames.add(field)) {
                    throw new IllegalArgumentException("Duplicate column name in field mappings: " + field);
                }
            }
        }

        if (hasConcepts) {
            for (ConceptMapping mapping : mappings.getConceptMappings()) {
                if (!columnNames.add(mapping.getColumn())) {
                    throw new IllegalArgumentException("Duplicate column name in field mappings: " + mapping.getColumn());
                }
            }
        }

        if (hasLookups) {
            for (LookupField lookup : mappings.getLookupFields()) {
                if (!columnNames.add(lookup.getColumn())) {
                    throw new IllegalArgumentException("Duplicate column name in field mappings: " + lookup.getColumn());
                }
            }
        }
    }
}
