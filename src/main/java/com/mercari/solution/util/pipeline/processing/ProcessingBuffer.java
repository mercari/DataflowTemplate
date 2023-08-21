package com.mercari.solution.util.pipeline.processing;

import com.google.common.collect.Lists;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class ProcessingBuffer {

    public Map<String, Integer> types;
    public Map<String, Integer> sizes;
    public Map<String, Integer> units;
    public Map<String, List<Long>> timestamps;
    public Map<String, List<Object>> values;

    // schema info
    public Map<String, List<String>> enums;

    public Map<String, Integer> getSizes() {
        return sizes;
    }

    public ProcessingBuffer() {
        this.types = new HashMap<>();
        this.sizes = new HashMap<>();
        this.units = new HashMap<>();

        this.timestamps = new HashMap<>();
        this.values = new HashMap<>();

        this.enums = new HashMap<>();
    }

    public ProcessingBuffer(final GenericRecord record) {
        this.types = (Map<String, Integer>) record.get("types");
        this.sizes = (Map<String, Integer>) record.get("sizes");
        this.units = (Map<String, Integer>) record.get("units");

        this.timestamps = (Map<String, List<Long>>) record.get("timestamps");
        this.values = (Map<String, List<Object>>) record.get("values");

        this.enums = (Map<String, List<String>>) record.get("enums");
    }

    public static ProcessingBuffer of(Map<String, org.apache.beam.sdk.schemas.Schema.FieldType> types, Map<String, Integer> sizes, Map<String, Integer> units) {
        final ProcessingBuffer buffer = new ProcessingBuffer();
        buffer.types = types.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> ProcessingBuffer.FieldType.of(e.getValue()).id));
        buffer.sizes = sizes;
        buffer.units = units;

        for(final var entry : types.entrySet()) {
            if(RowSchemaUtil.isLogicalTypeEnum(entry.getValue())) {
                final List<String> symbols = entry.getValue().getLogicalType(EnumerationType.class).getValues();
                buffer.enums.put(entry.getKey(), symbols);
            }
        }

        return buffer;
    }

    public void add(final String fieldName, final Object fieldValue, final Instant timestamp) {
        if(!values.containsKey(fieldName)) {
            values.put(fieldName, new ArrayList<>());
        }
        values.get(fieldName).add(fieldValue);

        if(!timestamps.containsKey(fieldName)) {
            timestamps.put(fieldName, new ArrayList<>());
        }
        timestamps.get(fieldName).add(timestamp.getMillis() * 1000L);
    }

    public Object get(final String name, final int index) {
        if(!values.containsKey(name)) {
            return null;
        }
        final List<Object> list = values.get(name);
        if(list.size() <= index) {
            return null;
        }
        final int i = list.size() - index - 1;
        return list.get(i);
    }

    public Object getAsValue(final String name, final int index) {
        final Object value = get(name, index);
        if(value == null) {
            return null;
        }
        final Integer type = this.types.get(name);
        if(type == null) {
            return null;
        }
        switch (FieldType.of(type)) {
            case int32:
            case int64:
            case float32:
            case float64:
            case string:
            case bool:
            case json:
            case enumeration:
                return value;
            case date: {
                final Integer epochDays = (Integer) value;
                return LocalDate.ofEpochDay(epochDays.longValue());
            }
            case time: {
                final Long microOfDay = (Long) value;
                return LocalTime.ofNanoOfDay(microOfDay * 1000L);
            }
            case timestamp: {
                final Long epochMicros = (Long) value;
                return Instant.ofEpochMilli(epochMicros / 1000L);
            }
            case bytes: {
                final ByteBuffer bytes = (ByteBuffer) value;
                return bytes.array();
            }
            case numeric:
            case array:
            case map:
            case struct:
            case datetime:
            default:
                return null;
        }
    }

    public Double getAsDouble(final String name, final int index) {
        final Integer type = types.get(name);
        if(type == null) {
            throw new IllegalStateException("ProcessingBuffer field: " + name + " has no type");
        }
        final Object value = get(name, index);
        if(value == null) {
            return null;
        }
        switch (FieldType.of(type)) {
            case bool:
                return ((Boolean) value) ? 1D : 0D;
            case date:
            case int32:
                return ((Integer) value).doubleValue();
            case time:
            case timestamp:
            case int64:
                return ((Long) value).doubleValue();
            case float32:
                return ((Float) value).doubleValue();
            case float64:
                return ((Double) value);
            case numeric:
                return ((BigDecimal) value).doubleValue();
            default:
                throw new IllegalArgumentException();
        }
    }

    public Integer getAsInteger(final String name, final int index) {
        final Integer type = types.get(name);
        if(type == null) {
            throw new IllegalStateException("ProcessingBuffer field: " + name + " has no type");
        }
        final Object value = get(name, index);
        if(value == null) {
            return null;
        }
        switch (FieldType.of(type)) {
            case bool:
                return ((Boolean) value) ? 1 : 0;
            case date:
            case int32:
            case enumeration:
                return ((Integer) value);
            case time:
            case timestamp:
            case int64:
                return ((Long) value).intValue();
            case float32:
                return ((Float) value).intValue();
            case float64:
                return ((Double) value).intValue();
            case numeric:
                return ((BigDecimal) value).intValue();
            default:
                throw new IllegalArgumentException("Not supported buffer type: " + FieldType.of(type));
        }
    }

    public List<Object> getValues(final String field, final int offset, final int size, final Processor.SizeUnit sizeUnit) {
        switch (sizeUnit) {
            case count: {
                final int m = Math.max(Math.min(size, values.get(field).size() - offset), 0);
                final List<Object> values = new ArrayList<>();
                for(int index=0; index<m; index++) {
                    final Object value = get(field, index + offset);
                    values.add(value);
                }
                return values;
            }
            case second:
            case minute:
            case hour:
            case day: {

            }
            case none:
            default:
                throw new IllegalArgumentException();
        }
    }

    public double[] getRowVector(final List<String> fields, final int index) {
        return getRowVector(fields, index, 0, 0D);
    }

    public double[] getRowVector(final List<String> fields, final int index, final int additionalSize, final double defaultValue) {
        int size = 0;
        final List<Double> list = new ArrayList<>();
        for(String field : fields) {
            if(FieldType.enumeration.id == types.get(field)) {
                final int enumSize = enums.get(field).size();
                final Integer value = getAsInteger(field, index);
                for(int i=0; i<enumSize; i++) {
                    if(value == null || i != value) {
                        list.add(0D);
                    } else {
                        list.add(1D);
                    }
                }
                size += enumSize;
            } else {
                final Double value = getAsDouble(field, index);
                list.add(value);
                size += 1;
            }
        }
        for(int i=0; i<additionalSize; i++) {
            list.add(defaultValue);
            size += 1;
        }

        final double[] doubles = new double[size];
        for(int i=0; i<size; i++) {
            doubles[i] = Optional.ofNullable(list.get(i)).orElse(Double.NaN);
        }

        return doubles;
    }

    public double[] getColVector(final String field, final int offset, final int size, final Processor.SizeUnit sizeUnit) {
        switch (sizeUnit) {
            case count: {
                final int m = Math.max(Math.min(size, values.get(field).size() - offset), 0);
                final double[] doubles = new double[m];
                for(int index=0; index<m; index++) {
                    final Double value = getAsDouble(field, index + offset);
                    doubles[index] = Optional.ofNullable(value).orElse(Double.NaN);
                }
                return doubles;
            }
            case second:
            case minute:
            case hour:
            case day: {

            }
            case none:
            default:
                throw new IllegalArgumentException();
        }
    }

    public double[][] getAsMatrix(final List<String> fields, int offset, int size, final Processor.SizeUnit sizeUnit) {
        return getAsMatrix(fields, offset, size, sizeUnit, false);
    }

    public double[][] getAsMatrix(final List<String> fields, int offset, int size, final Processor.SizeUnit sizeUnit, final boolean useIntercept) {
        switch (sizeUnit) {
            case count: {
                final int rows = Math.max(Math.min(size, values.get(fields.get(0)).size() - offset), 0);
                final double[][] matrix = new double[rows][];
                for(int index=0; index<rows; index++) {
                    int row = index + offset;
                    matrix[index] = getRowVector(fields, row, (useIntercept ? 1 : 0), 1D);
                }
                return matrix;
            }
            case second:
            case minute:
            case hour:
            case day: {

            }
            case none:
            default:
                throw new IllegalArgumentException();
        }
    }

    public void update(final Instant timestamp) {

        for(final String name : values.keySet()) {
            final int size = sizes.getOrDefault(name, 0);
            if(size <= 0) {
                if(this.values.containsKey(name)) {
                    this.values.get(name).clear();
                }
                if(this.timestamps.containsKey(name)) {
                    this.timestamps.get(name).clear();
                }
                continue;
            }
            final Integer unit = units.get(name);
            switch (Processor.SizeUnit.of(unit)) {
                case second:
                case minute:
                case hour:
                case day: {
                    // resize values
                    final long lowerBoundMicros = (timestamp.getMillis() * 1000L) - Processor.SizeUnit.getMicros(unit, size);
                    final int count = Long.valueOf(this.timestamps.get(name).stream().filter(t -> t > lowerBoundMicros).count()).intValue();
                    final List<?> list = Optional.ofNullable(this.values.get(name)).orElseGet(ArrayList::new);
                    this.values.put(name, new ArrayList<>(list.subList(0, Math.min(count, list.size()))));
                    // resize timestamps
                    if(this.timestamps.containsKey(name)) {
                        final List<Long> timestamps = this.timestamps.get(name);
                        this.timestamps.put(name, new ArrayList<>(timestamps.subList(0, Math.min(count, timestamps.size()))));
                    }
                    break;
                }
                case count:
                case none: {
                    if(this.values.containsKey(name)) {
                        final List<?> list = this.values.get(name);
                        if(!this.timestamps.containsKey(name)) {
                            throw new IllegalStateException();
                        }
                        final List<Long> timestamps = this.timestamps.get(name);
                        if(list.size() != timestamps.size()) {
                            throw new IllegalStateException();
                        }

                        final int fromIndex = Math.max(size, list.size()) - size;
                        final int toIndex = list.size();

                        // resize values
                        this.values.put(name, new ArrayList<>(list.subList(fromIndex, toIndex)));
                        // resize timestamps
                        this.timestamps.put(name, new ArrayList<>(timestamps.subList(fromIndex, toIndex)));
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    public ProcessingBuffer copy() {
        final ProcessingBuffer buffer = new ProcessingBuffer();

        buffer.types = new HashMap<>(this.types);
        buffer.sizes = new HashMap<>(this.sizes);
        buffer.units = new HashMap<>(this.units);

        buffer.timestamps = this.timestamps
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Lists.newArrayList(e.getValue())));

        buffer.values = this.values
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Lists.newArrayList(e.getValue())));

        buffer.enums = this.enums
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Lists.newArrayList(e.getValue())));

        return buffer;
    }

    public Map<String, Object> getLatestValues() {
        final Map<String, Object> output = new HashMap<>();
        for(final String name : values.keySet()) {
            final Object value = getAsValue(name, 0);
            output.put(name, value);
        }
        return output;
    }

    @Override
    public String toString() {
        final StringBuilder valuesMessage = new StringBuilder();
        for(Map.Entry<String, List<Object>> entry : this.values.entrySet()) {
            valuesMessage.append("    " + entry.getKey() + ": " + entry.getValue() + "\n");
        }
        final StringBuilder timestampsMessage = new StringBuilder();
        for(Map.Entry<String, List<Long>> entry : this.timestamps.entrySet()) {
            timestampsMessage.append("    " + entry.getKey() + ": [" + entry.getValue().stream()
                    .map(l -> l == null ? "null" : Instant.ofEpochMilli(l/1000L).toString())
                    .collect(Collectors.joining(", ")) + "]\n");
        }
        return "ProcessingBuffer: \n" +
                "  values:\n" + valuesMessage +
                "  timestamps\n" + timestampsMessage +
                "  types: " + types + "\n" +
                "  sizes: " + sizes + "\n" +
                "  units: " + units;
    }

    public static Schema schema() {
        final Schema typesSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        final Schema sizesSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        final Schema unitsSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        final Schema timestampsSchema = Schema.createMap(Schema.createArray(Schema.create(Schema.Type.LONG)));
        final Schema enumsSchema = Schema.createMap(Schema.createArray(Schema.create(Schema.Type.STRING)));

        final Schema multiValueSchema = Schema.createUnion(
                Schema.create(Schema.Type.DOUBLE),
                Schema.create(Schema.Type.LONG),
                Schema.create(Schema.Type.STRING),
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.FLOAT),
                Schema.create(Schema.Type.BOOLEAN),
                Schema.create(Schema.Type.BYTES),
                Schema.createArray(Schema.createArray(Schema.create(Schema.Type.DOUBLE)))
        );
        final Schema valuesSchema = Schema.createMap(Schema.createArray(multiValueSchema));

        return SchemaBuilder.record("ProcessingBuffer").fields()
                .name("types").type(typesSchema).noDefault()
                .name("sizes").type(sizesSchema).noDefault()
                .name("units").type(unitsSchema).noDefault()
                .name("timestamps").type(timestampsSchema).noDefault()
                .name("values").type(valuesSchema).noDefault()
                .name("enums").type(enumsSchema).noDefault()
                .endRecord();
    }

    public static Coder<ProcessingBuffer> coder() {
        //final org.apache.avro.Schema schema = schema();
        //return AvroCoder.of(ProcessingBuffer.class, schema, true);
        return ProcessingBufferCoder.of();
    }

    public enum FieldType {
        bool(0),
        string(1),
        json(2),
        bytes(3),
        int32(4),
        int64(5),
        float32(6),
        float64(7),
        numeric(8),
        time(9),
        date(10),
        datetime(11),
        timestamp(12),
        enumeration(13),
        matrix(14),
        array(100),
        map(200),
        struct(300);

        public final int id;

        FieldType(int id) {
            this.id = id;
        }

        public static FieldType of(int id) {
            for(FieldType type : values()) {
                if(type.id == id) {
                    return type;
                }
            }
            return null;
        }

        public static FieldType of(final org.apache.beam.sdk.schemas.Schema.FieldType fieldType) {
            switch (fieldType.getTypeName()) {
                case BOOLEAN:
                    return FieldType.bool;
                case INT32:
                    return FieldType.int32;
                case INT64:
                    return FieldType.int64;
                case FLOAT:
                    return FieldType.float32;
                case DOUBLE:
                    return FieldType.float64;
                case STRING:
                    return FieldType.string;
                case BYTES:
                    return FieldType.bytes;
                case DECIMAL:
                    return FieldType.numeric;
                case DATETIME:
                    return FieldType.timestamp;
                case LOGICAL_TYPE: {
                    if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                        return FieldType.date;
                    } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                        return FieldType.time;
                    } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                        return FieldType.enumeration;
                    } else {
                        throw new IllegalArgumentException();
                    }
                }
                case ITERABLE:
                case ARRAY: {
                    switch (fieldType.getCollectionElementType().getTypeName()) {
                        case ARRAY:
                        case ITERABLE: {
                            switch (fieldType.getCollectionElementType().getCollectionElementType().getTypeName()) {
                                case INT16:
                                case INT32:
                                case INT64:
                                case FLOAT:
                                case DOUBLE:
                                case DECIMAL:
                                    return FieldType.matrix;
                                default:
                                    throw new IllegalArgumentException();
                            }
                        }
                    }
                }
                case BYTE:
                case INT16:
                case ROW:
                case MAP:
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    public static class ProcessingBufferCoder extends StructuredCoder<ProcessingBuffer> {

        private final AvroCoder<ProcessingBuffer> coder;

        public ProcessingBufferCoder() {
            this.coder = AvroCoder.of(ProcessingBuffer.class, schema(), false);
        }

        public static ProcessingBufferCoder of() {
            return new ProcessingBufferCoder();
        }

        @Override
        public void encode(ProcessingBuffer value, OutputStream outStream) throws IOException {
            encode(value, outStream, Context.NESTED);
        }


        @Override
        public void encode(ProcessingBuffer value, OutputStream outStream, Context context) throws IOException {
            coder.encode(value, outStream, context);
        }

        @Override
        public ProcessingBuffer decode(InputStream inStream) throws IOException {
            final GenericRecord record = (GenericRecord) coder.decode(inStream, Context.NESTED);
            return new ProcessingBuffer(record);
        }

        @Override
        public ProcessingBuffer decode(InputStream inStream, Context context) throws  IOException {
            final GenericRecord record = (GenericRecord) coder.decode(inStream, context);
            return new ProcessingBuffer(record);
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
        public void registerByteSizeObserver(ProcessingBuffer value, ElementByteSizeObserver observer) throws Exception {
            coder.registerByteSizeObserver(value, observer);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            verifyDeterministic(this, "ProcessingBufferCoder is deterministic if all coders are deterministic");
        }
    }

}
