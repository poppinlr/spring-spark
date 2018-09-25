package com.spark.demo.script;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class RegisterSettlement implements Serializable {

    private String PER_NO;
    private String VISIT_TYPE;
    private Timestamp IN_HOSP_DATE;
}
