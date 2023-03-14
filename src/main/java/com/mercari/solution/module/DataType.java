package com.mercari.solution.module;

import java.io.Serializable;

public enum DataType implements Serializable {

    MESSAGE(0),
    ROW(1),
    AVRO(2),
    STRUCT(3),
    DOCUMENT(4),
    ENTITY(5),
    MUTATION(11),
    MUTATIONGROUP(12),
    UNKNOWN(99);

    private final int id;


    DataType(final int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static DataType of(final int id) {
        for(final DataType dataType : values()) {
            if(dataType.id == id) {
                return dataType;
            }
        }
        throw new IllegalArgumentException("No such enum object for DataType id: " + id);
    }
}
