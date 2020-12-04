package com.mercari.solution.module;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.converter.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;


public class FCollection<T> {

    private final String name;
    private final PCollection<T> collection;
    private final DataType dataType;
    private final Schema schema;
    private final org.apache.avro.Schema avroSchema;
    private final Type spannerType;

    private FCollection(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final Schema schema,
            final org.apache.avro.Schema avroSchema,
            final Type spannerType) {

        this.name = name;
        this.collection = pCollection;
        this.dataType = dataType;
        this.schema = schema;
        this.avroSchema = avroSchema;
        this.spannerType = spannerType;
    }

    public static <T> FCollection<T> of(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final Schema schema) {

        return new FCollection<>(name, pCollection, dataType, schema, null, null);
    }

    public static <T> FCollection<T> of(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final org.apache.avro.Schema avroSchema) {

        return new FCollection<>(name, pCollection, dataType, null, avroSchema, null);
    }

    public static <T> FCollection<T> of(
            final String name,
            final PCollection<T> pCollection,
            final DataType dataType,
            final Type spannerType) {

        return new FCollection<>(name, pCollection, dataType, null, null, spannerType);
    }

    public static <T> FCollection<T> update(final FCollection<T> base, final PCollection<T> pCollection) {
        return update(base, base.getName(), pCollection);
    }

    public static <T> FCollection<T> update(final FCollection<T> base, final String name, final PCollection<T> pCollection) {
        return new FCollection<>(name, pCollection, base.getDataType(), base.getSchema(), base.getAvroSchema(), base.getSpannerType());
    }

    public String getName() {
        return name;
    }

    public PCollection<T> getCollection() {
        return collection;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Schema getSchema() {
        if(this.schema != null) {
            return schema;
        } else if(this.avroSchema != null) {
            return RecordToRowConverter.convertSchema(avroSchema);
        } else if(this.spannerType != null) {
            return StructToRowConverter.convertSchema(spannerType);
        } else {
            throw new IllegalArgumentException("FCollection has no schemas!");
        }
    }

    public org.apache.avro.Schema getAvroSchema() {
        if(this.avroSchema != null) {
            return avroSchema;
        } else if(this.schema != null) {
            return RowToRecordConverter.convertSchema(schema);
        } else if(this.spannerType != null) {
            return StructToRecordConverter.convertSchema(spannerType);
        } else {
            throw new IllegalArgumentException("FCollection has no schemas!");
        }
    }

    public Type getSpannerType() {
        if(this.spannerType != null) {
            return spannerType;
        } else if(this.avroSchema != null) {
            return RecordToMutationConverter.convertSchema(this.avroSchema);
        } else if(this.schema != null) {
            return RowToMutationConverter.convertSchema(this.schema);
        } else {
            throw new IllegalArgumentException("FCollection has no SpannerType!");
        }
    }

    public TableSchema getTableSchema() {
        if(this.schema != null) {
            return RowToTableRowConverter.convertTableSchema(schema);
        } else if(this.avroSchema != null) {
            return RecordToTableRowConverter.convertSchema(avroSchema);
        } else if(this.spannerType != null) {
            return StructToTableRowConverter.convertSchema(spannerType);
        } else {
            throw new IllegalArgumentException("FCollection has no schemas!");
        }
    }

}
