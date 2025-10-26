package com.openmrs.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class TableColumn {
    @Column(name = "name")
    private String name;

    @Column(name = "type")
    private String type;
}
