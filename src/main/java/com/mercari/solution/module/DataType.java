package com.mercari.solution.module;

import java.io.Serializable;

public enum DataType implements Serializable {
    ROW,
    AVRO,
    STRUCT,
    ENTITY,
    DOCUMENT,
    MUTATION,
    MUTATIONGROUP,
    BIGTABLE,
    TEXT
}
