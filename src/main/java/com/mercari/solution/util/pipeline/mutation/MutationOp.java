package com.mercari.solution.util.pipeline.mutation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public enum MutationOp implements Serializable {

    INSERT(1),
    UPDATE(2),
    REPLACE(3),
    UPSERT(4),
    DELETE(5);

    private final int id;


    MutationOp(final int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static MutationOp of(final int id) {
        for(final MutationOp mutationOp : values()) {
            if(mutationOp.id == id) {
                return mutationOp;
            }
        }
        throw new IllegalArgumentException("No such enum object for MutationOp id: " + id);
    }

    public static List<String> symbols() {
        final List<String> symbols = new ArrayList<>();
        for(final MutationOp op : values()) {
            symbols.add(op.name());
        }
        return symbols;
    }

}
