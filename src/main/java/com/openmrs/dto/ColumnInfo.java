package com.openmrs.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnInfo {
    private String columnName;
    private String dataType;
    private Integer columnSize;
    private Integer decimalDigits;
    private boolean nullable;
    private boolean primaryKey;
}