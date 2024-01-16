package com.mercari.solution.util.pipeline.aggregation;

import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;


public class Accumulator {

    private static final Schema.FieldType FIELD_TYPE_MAP_DOUBLE = Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.DOUBLE);

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
    public Map<String, List<Boolean>> boolsList;

    public Map<String, Map<String, Double>> doublesMap;

    Accumulator() {

    }

    public Accumulator(final GenericRecord record) {
        this.empty = (Boolean) record.get("empty");
        this.ints = (Map<String, Integer>) record.get("ints");
        this.longs = (Map<String, Long>) record.get("longs");
        this.floats = (Map<String, Float>) record.get("floats");
        this.doubles = (Map<String, Double>) record.get("doubles");
        this.booleans = (Map<String, Boolean>) record.get("booleans");
        this.strings = (Map<String, String>) record.get("strings");

        this.intsList = (Map<String, List<Integer>>) record.get("intsList");
        this.longsList = (Map<String, List<Long>>) record.get("longsList");
        this.floatsList = (Map<String, List<Float>>) record.get("floatsList");
        this.doublesList = (Map<String, List<Double>>) record.get("doublesList");
        this.stringsList = (Map<String, List<String>>) record.get("stringsList");
        this.boolsList = (Map<String, List<Boolean>>) record.get("boolsList");

        this.doublesMap = (Map<String, Map<String, Double>>) record.get("doublesMap");
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
        accumulator.boolsList = new HashMap<>();

        accumulator.doublesMap = new HashMap<>();

        return accumulator;
    }

    public void put(Schema.FieldType fieldType, final String name, final Object value) {
        putValue(this, fieldType, name, value);
    }

    public void put(Schema.FieldType elementType, final String name, final List list) {
        putList(this, elementType, name, list);
    }

    public Object get(Schema.FieldType fieldType, final String name) {
        return getValue(this, fieldType, name);
    }

    public List<?> list(Schema.FieldType elementType, final String name) {
        return getList(this, elementType, name);
    }

    public void add(Schema.FieldType fieldType, final String name, final Object value) {
        addValue(this, fieldType, name, value);
    }

    public void putLong(String name, Long value) {
        putValue(this, Schema.FieldType.INT64, name, value);
    }

    public void putDouble(String name, Double value) {
        putValue(this, Schema.FieldType.DOUBLE, name, value);
    }

    public void putDoublesMap(String name, Map<String, Double> value) {
        putValue(this, FIELD_TYPE_MAP_DOUBLE, name, value);
    }

    public Long getLong(String fieldName) {
        return (Long) getValue(this, Schema.FieldType.INT64, fieldName);
    }

    public Double getDouble(String fieldName) {
        return (Double) getValue(this, Schema.FieldType.DOUBLE, fieldName);
    }

    public Map<String, Double> getDoublesMap(final String fieldName) {
        return (Map<String, Double>) getValue(this, FIELD_TYPE_MAP_DOUBLE, fieldName);
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
                    case DOUBLE -> {
                        return accumulator.doublesList.get(fieldName);
                    }
                    case FLOAT -> {
                        return accumulator.floatsList.get(fieldName);
                    }
                    case INT64, DATETIME -> {
                        return accumulator.longsList.get(fieldName);
                    }
                    case INT32 -> {
                        return accumulator.intsList.get(fieldName);
                    }
                    case STRING -> {
                        return accumulator.stringsList.get(fieldName);
                    }
                    case BOOLEAN -> {
                        return accumulator.boolsList.get(fieldName);
                    }
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                            return accumulator.intsList.get(fieldName);
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                            return accumulator.longsList.get(fieldName);
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                            return accumulator.stringsList.get(fieldName);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            case MAP: {
                return switch (fieldType.getMapValueType().getTypeName()) {
                    case DOUBLE -> accumulator.doublesMap.get(fieldName);
                    default -> throw new IllegalArgumentException();
                };
            }
            case BYTES:
            case DECIMAL:
            case INT16:
            case BYTE:
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
            case INT32 -> accumulator.ints.put(fieldName, (Integer) value);
            case INT64, DATETIME -> accumulator.longs.put(fieldName, (Long) value);
            case DOUBLE -> accumulator.doubles.put(fieldName, (Double) value);
            case FLOAT -> accumulator.floats.put(fieldName, (Float) value);
            case STRING -> accumulator.strings.put(fieldName, value.toString());
            case BOOLEAN -> accumulator.booleans.put(fieldName, (Boolean) value);
            case LOGICAL_TYPE -> {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    accumulator.ints.put(fieldName, (Integer) value);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    accumulator.longs.put(fieldName, (Long) value);
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    accumulator.strings.put(fieldName, value.toString());
                } else {
                    throw new IllegalStateException();
                }
            }
            case ARRAY, ITERABLE -> {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case DOUBLE -> accumulator.doublesList.put(fieldName, (List<Double>) value);
                    case FLOAT -> accumulator.floatsList.put(fieldName, (List<Float>) value);
                    case INT64, DATETIME -> accumulator.longsList.put(fieldName, (List<Long>) value);
                    case INT32 -> accumulator.intsList.put(fieldName, (List<Integer>) value);
                    case STRING -> {
                        final List<String> strings = ((List<Object>) value).stream().map(Object::toString).collect(Collectors.toList());
                        accumulator.stringsList.put(fieldName, strings);
                    }
                    case BOOLEAN -> accumulator.boolsList.put(fieldName, (List<Boolean>) value);
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            accumulator.intsList.put(fieldName, (List<Integer>) value);
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                            accumulator.longsList.put(fieldName, (List<Long>) value);
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                            final List<String> strings;
                            if (value == null) {
                                strings = null;
                            } else {
                                strings = ((List<Object>) value).stream().map(o -> o.toString()).collect(Collectors.toList());
                            }
                            accumulator.stringsList.put(fieldName, strings);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                    default -> throw new IllegalStateException();
                }
            }
            case MAP -> {
                switch (fieldType.getMapValueType().getTypeName()) {
                    case DOUBLE -> accumulator.doublesMap.put(fieldName, (Map<String, Double>) value);
                    default -> throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static List<?> getList(Accumulator accumulator, Schema.FieldType elementType, String fieldName) {
        switch (elementType.getTypeName()) {
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
            case BOOLEAN:
                return accumulator.boolsList.get(fieldName);
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(elementType)) {
                    return accumulator.intsList.get(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeTime(elementType)) {
                    return accumulator.longsList.get(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeEnum(elementType)) {
                    return accumulator.stringsList.get(fieldName);
                } else {
                    throw new IllegalStateException();
                }
            }
            default:
                throw new IllegalStateException();
        }
    }

    private static void putList(Accumulator accumulator, Schema.FieldType elementType, String fieldName, List value) {
        if(value == null) {
            removeKey(accumulator, elementType, fieldName);
            return;
        }
        switch (elementType.getTypeName()) {
            case DOUBLE -> accumulator.doublesList.put(fieldName, (List<Double>) value);
            case FLOAT -> accumulator.floatsList.put(fieldName, (List<Float>) value);
            case INT64, DATETIME -> accumulator.longsList.put(fieldName, (List<Long>) value);
            case INT32 -> accumulator.intsList.put(fieldName, (List<Integer>) value);
            case STRING -> {
                final List<String> strings = ((List<Object>) value).stream().map(o -> o == null ? null : o.toString()).collect(Collectors.toList());
                accumulator.stringsList.put(fieldName, strings);
            }
            case BOOLEAN -> accumulator.boolsList.put(fieldName, (List<Boolean>) value);
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(elementType)) {
                    accumulator.intsList.put(fieldName, (List<Integer>) value);
                } else if (RowSchemaUtil.isLogicalTypeTime(elementType)) {
                    accumulator.longsList.put(fieldName, (List<Long>) value);
                } else if (RowSchemaUtil.isLogicalTypeEnum(elementType)) {
                    final List<String> strings = ((List<Object>) value).stream().map(o -> o == null ? null : o.toString()).collect(Collectors.toList());
                    accumulator.stringsList.put(fieldName, strings);
                } else {
                    throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static void addValue(Accumulator accumulator, Schema.FieldType fieldType, String fieldName, Object value) {
        switch (fieldType.getTypeName()) {
            case INT32 -> {
                final List<Integer> intsList = Optional.ofNullable(accumulator.intsList.get(fieldName)).orElseGet(ArrayList::new);
                intsList.add((Integer) value);
                accumulator.intsList.put(fieldName, intsList);
            }
            case INT64, DATETIME -> {
                final List<Long> longsList = Optional.ofNullable(accumulator.longsList.get(fieldName)).orElseGet(ArrayList::new);
                longsList.add((Long) value);
                accumulator.longsList.put(fieldName, longsList);
            }
            case DOUBLE -> {
                final List<Double> doublesList = Optional.ofNullable(accumulator.doublesList.get(fieldName)).orElseGet(ArrayList::new);
                doublesList.add((Double) value);
                accumulator.doublesList.put(fieldName, doublesList);
            }
            case FLOAT -> {
                final List<Float> floatsList = Optional.ofNullable(accumulator.floatsList.get(fieldName)).orElseGet(ArrayList::new);
                floatsList.add((Float) value);
                accumulator.floatsList.put(fieldName, floatsList);
            }
            case STRING -> {
                final List<String> stringsList = Optional.ofNullable(accumulator.stringsList.get(fieldName)).orElseGet(ArrayList::new);
                stringsList.add(value == null ? null : value.toString());
                accumulator.stringsList.put(fieldName, stringsList);
            }
            case BOOLEAN -> {
                final List<Boolean> boolsList = Optional.ofNullable(accumulator.boolsList.get(fieldName)).orElseGet(ArrayList::new);
                boolsList.add((Boolean) value);
                accumulator.boolsList.put(fieldName, boolsList);
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    final List<Integer> intsList = Optional.ofNullable(accumulator.intsList.get(fieldName)).orElseGet(ArrayList::new);
                    intsList.add((Integer) value);
                    accumulator.intsList.put(fieldName, intsList);
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    final List<Long> longsList = Optional.ofNullable(accumulator.longsList.get(fieldName)).orElseGet(ArrayList::new);
                    longsList.add((Long) value);
                    accumulator.longsList.put(fieldName, longsList);
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final List<String> stringsList = Optional.ofNullable(accumulator.stringsList.get(fieldName)).orElseGet(ArrayList::new);
                    if (value == null) {
                        stringsList.add(null);
                    } else {
                        stringsList.add(value.toString());
                    }
                    accumulator.stringsList.put(fieldName, stringsList);
                } else {
                    throw new IllegalStateException();
                }
            }
            default ->
                    throw new IllegalStateException("Not supported add type: " + fieldType.getTypeName() + " for field: " + fieldName);
        }
    }

    public static void addList(Accumulator accumulator, Schema.FieldType elementType, String fieldName, List<?> list) {
        if(list == null || list.size() == 0) {
            return;
        }
        switch (elementType.getTypeName()) {
            case INT32 -> {
                final List<Integer> intsList = Optional.ofNullable(accumulator.intsList.get(fieldName)).orElseGet(ArrayList::new);
                for (Object value : list) {
                    intsList.add((Integer) value);
                }
                accumulator.intsList.put(fieldName, intsList);
            }
            case INT64, DATETIME -> {
                final List<Long> longsList = Optional.ofNullable(accumulator.longsList.get(fieldName)).orElseGet(ArrayList::new);
                for (Object value : list) {
                    longsList.add((Long) value);
                }
                accumulator.longsList.put(fieldName, longsList);
            }
            case DOUBLE -> {
                final List<Double> doublesList = Optional.ofNullable(accumulator.doublesList.get(fieldName)).orElseGet(ArrayList::new);
                for (Object value : list) {
                    doublesList.add((Double) value);
                }
                accumulator.doublesList.put(fieldName, doublesList);
            }
            case FLOAT -> {
                final List<Float> floatsList = Optional.ofNullable(accumulator.floatsList.get(fieldName)).orElseGet(ArrayList::new);
                for (Object value : list) {
                    floatsList.add((Float) value);
                }
                accumulator.floatsList.put(fieldName, floatsList);
            }
            case STRING -> {
                final List<String> stringsList = Optional.ofNullable(accumulator.stringsList.get(fieldName)).orElseGet(ArrayList::new);
                for (Object value : list) {
                    stringsList.add((String) value);
                }
                accumulator.stringsList.put(fieldName, stringsList);
            }
            case BOOLEAN -> {
                final List<Boolean> boolsList = Optional.ofNullable(accumulator.boolsList.get(fieldName)).orElseGet(ArrayList::new);
                for (Object value : list) {
                    boolsList.add((Boolean) value);
                }
                accumulator.boolsList.put(fieldName, boolsList);
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(elementType)) {
                    final List<Integer> intsList = Optional.ofNullable(accumulator.intsList.get(fieldName)).orElseGet(ArrayList::new);
                    for (Object value : list) {
                        intsList.add((Integer) value);
                    }
                    accumulator.intsList.put(fieldName, intsList);
                } else if (RowSchemaUtil.isLogicalTypeTime(elementType)) {
                    final List<Long> longsList = Optional.ofNullable(accumulator.longsList.get(fieldName)).orElseGet(ArrayList::new);
                    for (Object value : list) {
                        longsList.add((Long) value);
                    }
                    accumulator.longsList.put(fieldName, longsList);
                } else if (RowSchemaUtil.isLogicalTypeEnum(elementType)) {
                    final List<String> stringsList = Optional.ofNullable(accumulator.stringsList.get(fieldName)).orElseGet(ArrayList::new);
                    for (Object value : list) {
                        stringsList.add((String) value);
                    }
                    accumulator.stringsList.put(fieldName, stringsList);
                } else {
                    throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException("Not supported type: " + elementType);
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
                    case DOUBLE -> accumulator.doublesList.remove(fieldName);
                    case FLOAT -> accumulator.floatsList.remove(fieldName);
                    case INT64, DATETIME -> accumulator.longsList.remove(fieldName);
                    case INT32 -> accumulator.intsList.remove(fieldName);
                    case STRING -> accumulator.stringsList.remove(fieldName);
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            accumulator.intsList.remove(fieldName);
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                            accumulator.longsList.remove(fieldName);
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                            accumulator.stringsList.remove(fieldName);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                    default -> throw new IllegalStateException();
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

    public static Object convertNumberValue(Schema.FieldType fieldType, Double value) {
        if(value == null) {
            return null;
        }
        return switch (fieldType.getTypeName()) {
            case FLOAT -> value.floatValue();
            case DOUBLE -> value;
            case INT16 -> value.shortValue();
            case INT32 -> value.intValue();
            case INT64 -> value.longValue();
            default -> null;
        };
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
        for(final Map.Entry<String, Map<String, Double>> entry : base.doublesMap.entrySet()) {
            if(outputFields == null || outputFields.contains(entry.getKey())) {
                output.doublesMap.put(entry.getKey(), new HashMap<>(entry.getValue()));
            }
        }

        return output;
    }

    public static org.apache.avro.Schema schema() {
        final org.apache.avro.Schema emptySchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);

        final org.apache.avro.Schema intsSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        final org.apache.avro.Schema longsSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        final org.apache.avro.Schema floatsSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
        final org.apache.avro.Schema doublesSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE));
        final org.apache.avro.Schema booleansSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));
        final org.apache.avro.Schema stringsSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));

        final org.apache.avro.Schema intsListSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createArray(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));
        final org.apache.avro.Schema longsListSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createArray(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));
        final org.apache.avro.Schema floatsListSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createArray(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));
        final org.apache.avro.Schema doublesListSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createArray(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));
        final org.apache.avro.Schema stringsListSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createArray(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));
        final org.apache.avro.Schema boolsListSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createArray(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));

        final org.apache.avro.Schema doublesMapSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.createMap(
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))));


        return SchemaBuilder.record("Accumulator").fields()
                .name("empty").type(emptySchema).noDefault()
                .name("ints").type(intsSchema).noDefault()
                .name("longs").type(longsSchema).noDefault()
                .name("floats").type(floatsSchema).noDefault()
                .name("doubles").type(doublesSchema).noDefault()
                .name("booleans").type(booleansSchema).noDefault()
                .name("strings").type(stringsSchema).noDefault()
                .name("intsList").type(intsListSchema).noDefault()
                .name("longsList").type(longsListSchema).noDefault()
                .name("floatsList").type(floatsListSchema).noDefault()
                .name("doublesList").type(doublesListSchema).noDefault()
                .name("stringsList").type(stringsListSchema).noDefault()
                .name("boolsList").type(boolsListSchema).noDefault()
                .name("doublesMap").type(doublesMapSchema).noDefault()
                .endRecord();
    }

    public static class AccumulatorCoder extends StructuredCoder<Accumulator> {

        private final AvroCoder<Accumulator> coder;

        public AccumulatorCoder() {
            this.coder = AvroCoder.of(Accumulator.class, schema(), false);
        }

        public static AccumulatorCoder of() {
            return new AccumulatorCoder();
        }

        @Override
        public void encode(Accumulator value, OutputStream outStream) throws IOException {
            encode(value, outStream, Context.NESTED);
        }


        @Override
        public void encode(Accumulator value, OutputStream outStream, Context context) throws IOException {
            coder.encode(value, outStream, context);
        }

        @Override
        public Accumulator decode(InputStream inStream) throws IOException {
            final GenericRecord record = (GenericRecord) coder.decode(inStream, Context.NESTED);
            return new Accumulator(record);
        }

        @Override
        public Accumulator decode(InputStream inStream, Context context) throws  IOException {
            final GenericRecord record = (GenericRecord) coder.decode(inStream, context);
            return new Accumulator(record);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public List<? extends Coder<?>> getComponents() {
            final List<Coder<?>> coders = new ArrayList<>();
            coders.add(coder);
            return coders;
        }

        @Override
        public void registerByteSizeObserver(Accumulator value, ElementByteSizeObserver observer) throws Exception {
            coder.registerByteSizeObserver(value, observer);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            verifyDeterministic(this, "Accumulator is deterministic if all coders are deterministic");
        }
    }

    public static Coder<Accumulator> coder() {
        return AccumulatorCoder.of();
    }

    @Override
    public String toString() {
        final StringBuilder valuesMessage = new StringBuilder();
        for(Map.Entry<String, Integer> entry : this.ints.entrySet()) {
            valuesMessage.append("    ints." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, Long> entry : this.longs.entrySet()) {
            valuesMessage.append("    longs." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, Float> entry : this.floats.entrySet()) {
            valuesMessage.append("    floats." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, Double> entry : this.doubles.entrySet()) {
            valuesMessage.append("    doubles." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, Boolean> entry : this.booleans.entrySet()) {
            valuesMessage.append("    booleans." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, String> entry : this.strings.entrySet()) {
            valuesMessage.append("    strings." + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        for(Map.Entry<String, List<Integer>> entry : this.intsList.entrySet()) {
            valuesMessage.append("    intsList." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, List<Long>> entry : this.longsList.entrySet()) {
            valuesMessage.append("    longsList." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, List<Float>> entry : this.floatsList.entrySet()) {
            valuesMessage.append("    floatsList." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, List<Double>> entry : this.doublesList.entrySet()) {
            valuesMessage.append("    doublesList." + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        for(Map.Entry<String, List<String>> entry : this.stringsList.entrySet()) {
            valuesMessage.append("    stringsList." + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        return "aggregation.Accumulator: \n" +
                "  values:\n" + valuesMessage +
                "  empty: " + empty;
    }

}
