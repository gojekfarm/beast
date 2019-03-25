package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Setter
@Getter
public class UpdateBQTableRequest {
    private String project;
    private String proto;
    private String table;
    private String dataset;
}
