package com.mercari.solution.module;

import java.io.Serializable;

public enum DataType implements Serializable {
    ROW,
    AVRO,
    STRUCT,
    TEXT,
    ENTITY,
    MUTATION,
    BIGTABLE
}
