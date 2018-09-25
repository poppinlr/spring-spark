package com.spark.demo.script;

import lombok.Data;

import java.io.Serializable;

@Data
public class ChargeDetail implements Serializable {

    private String perNo;
    private String visit_type;
}
