package com.mercari.solution.module;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public enum DataType implements Serializable {

    ELEMENT(0),
    ROW(1),
    AVRO(2),
    STRUCT(3),
    DOCUMENT(4),
    ENTITY(5),
    MESSAGE(9),
    UNIFIEDMUTATION(10),
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

    public static List<String> symbols() {
        final List<String> symbols = new ArrayList<>();
        for(final DataType type : values()) {
            symbols.add(type.name());
        }
        return symbols;
    }
}
