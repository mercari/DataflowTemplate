package com.mercari.solution.util.pipeline.aggregation;

import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


@DefaultCoder(AvroCoder.class)
public class Accumulator implements Serializable {

    public Boolean empty;

    public Map<String, Integer> ints;
    public Map<String, Long> longs;
    public Map<String, Float> floats;
    public Map<String, Double> doubles;
    public Map<String, Boolean> booleans;
    public Map<String, String> strings;

    public Map<String, List<Integer>> intsList;
    public Map<String, List<Long>> longsList;
    public Map<String, List<Float>> floatsList;
    public Map<String, List<Double>> doublesList;
    public Map<String, List<String>> stringsList;

    Accumulator() {

    }

    public static Accumulator of() {
        final Accumulator accumulator = new Accumulator();

        accumulator.empty = true;
        accumulator.ints = new HashMap<>();
        accumulator.longs = new HashMap<>();
        accumulator.floats = new HashMap<>();
        accumulator.doubles = new HashMap<>();
        accumulator.strings = new HashMap<>();
        accumulator.booleans = new HashMap<>();

        accumulator.intsList = new HashMap<>();
        accumulator.longsList = new HashMap<>();
        accumulator.floatsList = new HashMap<>();
        accumulator.doublesList = new HashMap<>();
        accumulator.stringsList = new HashMap<>();

        return accumulator;
    }

    public void put(Schema.FieldType fieldType, final String name, final Object value) {
        putValue(this, fieldType, name, value);
    }

    public Object get(Schema.FieldType fieldType, final String name) {
        return getValue(this, fieldType, name);
    }

    public void putLong(String name, Long value) {
        putValue(this, Schema.FieldType.INT64, name, value);
    }

    public void putDouble(String name, Double value) {
        putValue(this, Schema.FieldType.DOUBLE, name, value);
    }

    public Long getLong(String fieldName) {
        return (Long) getValue(this, Schema.FieldType.INT64, fieldName);
    }

    public Double getDouble(String fieldName) {
        return (Double) getValue(this, Schema.FieldType.DOUBLE, fieldName);
    }

    public static Object getValue(Accumulator accumulator, Schema.FieldType fieldType, String fieldName) {
        switch (fieldType.getTypeName()) {
            case DOUBLE:
                return accumulator.doubles.get(fieldName);
            case FLOAT:
                return accumulator.floats.get(fieldName);
            case INT64:
            case DATETIME:
                return accumulator.longs.get(fieldName);
            case INT32:
                return accumulator.ints.get(fieldName);
            case STRING:
                return accumulator.strings.get(fieldName);
            case BOOLEAN:
                return accumulator.booleans.get(fieldName);
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return accumulator.ints.get(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return accumulator.longs.get(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return accumulator.strings.get(fieldName);
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE:
            case ARRAY: {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case DOUBLE:
                        return accumulator.doublesList.get(fieldName);
                    case FLOAT:
                        return accumulator.floatsList.get(fieldName);
                    case INT64:
                    case DATETIME:
                        return accumulator.longsList.get(fieldName);
                    case INT32:
                        return accumulator.intsList.get(fieldName);
                    case STRING:
                        return accumulator.stringsList.get(fieldName);
                    case LOGICAL_TYPE: {
                        if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                            return accumulator.intsList.get(fieldName);
                        } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                            return accumulator.longsList.get(fieldName);
                        } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                            return accumulator.stringsList.get(fieldName);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            case BYTES:
            case DECIMAL:
            case INT16:
            case BYTE:
            case MAP:
            case ROW:
            default:
                throw new IllegalStateException();
        }
    }

    public static void putValue(Accumulator accumulator, Schema.FieldType fieldType, String fieldName, Object value) {
        if(value == null) {
            removeKey(accumulator, fieldType, fieldName);
            return;
        }
        switch (fieldType.getTypeName()) {
            case INT32:
                accumulator.ints.put(fieldName, (Integer) value);
                break;
            case INT64:
            case DATETIME:
                accumulator.longs.put(fieldName, (Long) value);
                break;
            case DOUBLE:
                accumulator.doubles.put(fieldName, (Double) value);
                break;
            case FLOAT:
                accumulator.floats.put(fieldName, (Float) value);
                break;
            case STRING:
                accumulator.strings.put(fieldName, value.toString());
                break;
            case BOOLEAN:
                accumulator.booleans.put(fieldName, (Boolean) value);
                break;
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    accumulator.ints.put(fieldName, (Integer) value);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    accumulator.longs.put(fieldName, (Long) value);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    if(value == null) {
                        accumulator.strings.put(fieldName, null);
                    } else {
                        accumulator.strings.put(fieldName, value.toString());
                    }
                } else {
                    throw new IllegalStateException();
                }
                break;
            }
            case ARRAY:
            case ITERABLE: {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case DOUBLE:
                        accumulator.doublesList.put(fieldName, (List<Double>) value);
                        break;
                    case FLOAT:
                        accumulator.floatsList.put(fieldName, (List<Float>) value);
                        break;
                    case INT64:
                    case DATETIME:
                        accumulator.longsList.put(fieldName, (List<Long>) value);
                        break;
                    case INT32:
                        accumulator.intsList.put(fieldName, (List<Integer>) value);
                        break;
                    case STRING: {
                        final List<String> strings = ((List<Object>) value).stream().map(Object::toString).collect(Collectors.toList());
                        accumulator.stringsList.put(fieldName, strings);
                        break;
                    }
                    case LOGICAL_TYPE: {
                        if(RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            accumulator.intsList.put(fieldName, (List<Integer>) value);
                        } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                            accumulator.longsList.put(fieldName, (List<Long>) value);
                        } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                            final List<String> strings;
                            if(value == null) {
                                strings = null;
                            } else {
                                strings = ((List<Object>)value).stream().map(o -> o.toString()).collect(Collectors.toList());
                            }
                            accumulator.stringsList.put(fieldName, strings);
                        } else {
                            throw new IllegalStateException();
                        }
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
            case BYTE:
            case INT16:
            case DECIMAL:
            case BYTES:
            case MAP:
            default:
                throw new IllegalStateException();
        }
    }

    public static void removeKey(Accumulator accumulator, Schema.FieldType fieldType, String fieldName) {
        switch (fieldType.getTypeName()) {
            case INT32:
                accumulator.ints.remove(fieldName);
                break;
            case INT64:
            case DATETIME:
                accumulator.longs.remove(fieldName);
                break;
            case DOUBLE:
                accumulator.doubles.remove(fieldName);
                break;
            case FLOAT:
                accumulator.floats.remove(fieldName);
                break;
            case STRING:
                accumulator.strings.remove(fieldName);
                break;
            case BOOLEAN:
                accumulator.booleans.remove(fieldName);
                break;
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    accumulator.ints.remove(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    accumulator.longs.remove(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    accumulator.strings.remove(fieldName);
                } else {
                    throw new IllegalStateException();
                }
                break;
            }
            case ARRAY:
            case ITERABLE: {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case DOUBLE:
                        accumulator.doublesList.remove(fieldName);
                        break;
                    case FLOAT:
                        accumulator.floatsList.remove(fieldName);
                        break;
                    case INT64:
                    case DATETIME:
                        accumulator.longsList.remove(fieldName);
                        break;
                    case INT32:
                        accumulator.intsList.remove(fieldName);
                        break;
                    case STRING:
                        accumulator.stringsList.remove(fieldName);
                        break;
                    case LOGICAL_TYPE: {
                        if(RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            accumulator.intsList.remove(fieldName);
                        } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                            accumulator.longsList.remove(fieldName);
                        } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                            accumulator.stringsList.remove(fieldName);
                        } else {
                            throw new IllegalStateException();
                        }
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
            case BYTE:
            case INT16:
            case DECIMAL:
            case BYTES:
            case MAP:
            default:
                throw new IllegalStateException();
        }
    }

    public static Accumulator copy(Accumulator base, Set<String> outputFields) {
        final Accumulator output = Accumulator.of();
        output.empty = base.empty;
        for(final Map.Entry<String, Double> entry : base.doubles.entrySet()) {
            if(outputFields == null || outputFields.contains(entry.getKey())) {
                output.doubles.put(entry.getKey(), entry.getValue());
            }
        }
        for(final Map.Entry<String, Long> entry : base.longs.entrySet()) {
            if(outputFields == null || outputFields.contains(entry.getKey())) {
                output.longs.put(entry.getKey(), entry.getValue());
            }
        }
        for(final Map.Entry<String, Integer> entry : base.ints.entrySet()) {
            if(outputFields == null || outputFields.contains(entry.getKey())) {
                output.ints.put(entry.getKey(), entry.getValue());
            }
        }
        for(final Map.Entry<String, String> entry : base.strings.entrySet()) {
            if(outputFields == null || outputFields.contains(entry.getKey())) {
                output.strings.put(entry.getKey(), entry.getValue());
            }
        }
        for(final Map.Entry<String, List<Double>> entry : base.doublesList.entrySet()) {
            if(outputFields == null || outputFields.contains(entry.getKey())) {
                output.doublesList.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
        }

        return output;
    }

}
