package com.mercari.solution.util.pipeline.union;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.Row;

import java.util.Objects;

public class UnionValue {

    private final int index;
    private final DataType type;
    private final long epochMillis;
    private final Object value;

    public UnionValue(int index, DataType dataType, long epochMillis, Object value) {
        this.index = index;
        this.type = dataType;
        this.epochMillis = epochMillis;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public DataType getType() {
        return type;
    }

    public long getEpochMillis() {
        return epochMillis;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if(o == null || getClass() != o.getClass()) {
            return false;
        }

        final UnionValue that = (UnionValue) o;

        if(index != that.index) {
            return false;
        }
        if(epochMillis != that.epochMillis) {
            return false;
        }
        return Objects.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        return 31 * index + (value != null ? value.hashCode() : 0);
    }

    public static Object getFieldValue(final UnionValue unionValue, final String field) {
        if(unionValue.value == null) {
            return null;
        }
        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                return row.getValue(field);
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return record.get(field);
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.getValue(struct, field);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getValue(entity, field);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public Double getAsDouble(final String field) {
        return getAsDouble(this, field);
    }

    public static Double getAsDouble(final UnionValue unionValue, final String field) {
        if(unionValue.value == null) {
            return null;
        }
        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.getAsDouble(row, field);
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.getAsDouble(record, field);
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.getAsDouble(struct, field);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getAsDouble(entity, field);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }

    }

}
