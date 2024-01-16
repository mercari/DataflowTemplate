package com.mercari.solution.util.pipeline.union;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.*;

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
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.getValue(document, field);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getValue(entity, field);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public String getString(final String field) {
        return getAsString(this, field);
    }
    public Double getDouble(final String field) {
        return getAsDouble(this, field);
    }

    public Map<String, Object> getMap() {
        return getAsMap(this, null);
    }

    public Map<String, Object> getMap(final Collection<String> fields) {
        return getAsMap(this, fields);
    }

    public Map<String, Double> getDoubleMap(final Collection<String> fields) {
        return getAsDoubleMap(this, fields);
    }

    public static String getAsString(final UnionValue unionValue, final String field) {
        if(unionValue.value == null) {
            return null;
        }
        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.getAsString(row, field);
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.getAsString(record, field);
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.getAsString(struct, field);
            }
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.getAsString(document, field);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getAsString(entity, field);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
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
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.getAsDouble(document, field);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.getAsDouble(entity, field);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }

    }

    public static Map<String, Object> getAsMap(final UnionValue unionValue, final Collection<String> fields) {
        if(unionValue.value == null) {
            return new HashMap<>();
        }
        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                return RowToMapConverter.convertWithFields(row, fields);
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return RecordToMapConverter.convertWithFields(record, fields);
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                return StructToMapConverter.convertWithFields(struct, fields);
            }
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                return DocumentToMapConverter.convertWithFields(document, fields);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntityToMapConverter.convertWithFields(entity, fields);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public static Map<String, Object> asPrimitiveMap(final UnionValue unionValue) {
        if(unionValue.value == null) {
            return new HashMap<>();
        }
        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                return RowSchemaUtil.asPrimitiveMap(row);
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                return AvroSchemaUtil.asPrimitiveMap(record);
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                return StructSchemaUtil.asPrimitiveMap(struct);
            }
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                return DocumentSchemaUtil.asPrimitiveMap(document);
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                return EntitySchemaUtil.asPrimitiveMap(entity);
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }
    }

    public static Map<String, Double> getAsDoubleMap(final UnionValue unionValue, final Collection<String> fields) {
        final Map<String, Double> doubles = new HashMap<>();
        if(unionValue.value == null) {
            return doubles;
        }
        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                for(final String field : fields) {
                    doubles.put(field, RowSchemaUtil.getAsDouble(row, field));
                }
                break;
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                for(final String field : fields) {
                    doubles.put(field, AvroSchemaUtil.getAsDouble(record, field));
                }
                break;
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                for(final String field : fields) {
                    doubles.put(field, StructSchemaUtil.getAsDouble(struct, field));
                }
                break;
            }
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                for(final String field : fields) {
                    doubles.put(field, DocumentSchemaUtil.getAsDouble(document, field));
                }
                break;
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                for(final String field : fields) {
                    doubles.put(field, EntitySchemaUtil.getAsDouble(entity, field));
                }
                break;
            }
            default:
                throw new IllegalStateException("Union not supported data type: " + unionValue.type.name());
        }

        return doubles;
    }

    public static String getAsJson(final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToJsonConverter.convert((Row) unionValue.getValue());
            case AVRO -> RecordToJsonConverter.convert((GenericRecord) unionValue.getValue());
            case STRUCT -> StructToJsonConverter.convert((Struct) unionValue.getValue());
            case DOCUMENT -> DocumentToJsonConverter.convert((Document) unionValue.getValue());
            case ENTITY -> EntityToJsonConverter.convert((Entity) unionValue.getValue());
            case MUTATION -> MutationToJsonConverter.convertJsonString((Mutation) unionValue.getValue());
            case MUTATIONGROUP -> MutationToJsonConverter.convertJsonString((MutationGroup) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static GenericRecord getAsRecord(final org.apache.avro.Schema schema, final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToRecordConverter.convert(schema, (Row) unionValue.getValue());
            case AVRO -> (GenericRecord) unionValue.getValue();
            case STRUCT -> StructToRecordConverter.convert(schema, (Struct) unionValue.getValue());
            case DOCUMENT -> DocumentToRecordConverter.convert(schema, (Document) unionValue.getValue());
            case ENTITY -> EntityToRecordConverter.convert(schema, (Entity) unionValue.getValue());
            case MUTATION -> MutationToRecordConverter.convert(schema, (Mutation) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static DynamicMessage getAsProtoMessage(final Descriptors.Descriptor messageDescriptor, final UnionValue unionValue) {
        if(unionValue == null) {
            return null;
        }
        return switch (unionValue.getType()) {
            case ROW -> RowToProtoConverter.convert(messageDescriptor, (Row) unionValue.getValue());
            case AVRO -> RecordToProtoConverter.convert(messageDescriptor, (GenericRecord) unionValue.getValue());
            case STRUCT -> StructToProtoConverter.convert(messageDescriptor, (Struct) unionValue.getValue());
            case ENTITY -> EntityToProtoConverter.convert(messageDescriptor, (Entity) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };
    }

    public static Object merge(final UnionValue unionValue, Object schema, Map<String, Object> updates, DataType dataType) {
        if(unionValue.value == null) {
            return null;
        }

        switch (unionValue.type) {
            case ROW: {
                final Row row = (Row) unionValue.value;
                switch (dataType) {
                    case ROW:
                        return RowSchemaUtil.merge((Schema) schema, row, updates);
                    case AVRO: {
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = RowToRecordConverter.convert(avroSchema, row);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    }
                    default:
                        throw new IllegalStateException("Not supported conversion. from beam row to " + dataType.name());
                }
            }
            case AVRO: {
                final GenericRecord record = (GenericRecord) unionValue.value;
                switch (dataType) {
                    case ROW: {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = RecordToRowConverter.convert(rowSchema, record);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO:
                        return AvroSchemaUtil.merge((org.apache.avro.Schema) schema, record, updates);
                    default:
                        throw new IllegalStateException("Not supported conversion. from avro record to " + dataType.name());
                }
            }
            case STRUCT: {
                final Struct struct = (Struct) unionValue.value;
                switch (dataType) {
                    case ROW: {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = StructToRowConverter.convert(rowSchema, struct);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO:
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = StructToRecordConverter.convert(avroSchema, struct);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    default:
                        throw new IllegalStateException("Not supported conversion. from spanner struct to " + dataType.name());
                }
            }
            case DOCUMENT: {
                final Document document = (Document) unionValue.value;
                switch (dataType) {
                    case ROW: {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = DocumentToRowConverter.convert(rowSchema, document);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO:
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = DocumentToRecordConverter.convert(avroSchema, document);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    default:
                        throw new IllegalStateException("Not supported conversion. from firestore document to " + dataType.name());
                }
            }
            case ENTITY: {
                final Entity entity = (Entity) unionValue.value;
                switch (dataType) {
                    case ROW: {
                        final Schema rowSchema = (Schema) schema;
                        final Row row = EntityToRowConverter.convert(rowSchema, entity);
                        return RowSchemaUtil.merge(rowSchema, row, updates);
                    }
                    case AVRO:
                        final org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema;
                        final GenericRecord record = EntityToRecordConverter.convert(avroSchema, entity);
                        return AvroSchemaUtil.merge(avroSchema, record, updates);
                    default:
                        throw new IllegalStateException("Not supported conversion. from datastore entity to " + dataType.name());
                }
            }
            default:
                throw new IllegalStateException("Union.merge not supported data type: " + unionValue.type.name());
        }

    }

}
