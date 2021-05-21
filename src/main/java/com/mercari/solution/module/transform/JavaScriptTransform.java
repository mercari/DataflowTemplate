package com.mercari.solution.module.transform;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.Timestamp;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.EntityToMapConverter;
import com.mercari.solution.util.converter.RecordToMapConverter;
import com.mercari.solution.util.converter.RowToMapConverter;
import com.mercari.solution.util.converter.StructToMapConverter;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;


public class JavaScriptTransform implements TransformModule {

    private static final String OUTPUT_SUFFIX_FAILURES = ".failures";

    private class JavaScriptTransformParameters implements Serializable {

        private String script;
        private List<Mapping> mappings;

        // For stateful processing
        private String stateUpdateFunction;
        private List<String> groupFields;

        private Boolean failFast;

        public String getScript() {
            return script;
        }

        public void setScript(String script) {
            this.script = script;
        }

        public List<Mapping> getMappings() {
            return mappings;
        }

        public void setMappings(List<Mapping> mappings) {
            this.mappings = mappings;
        }

        public String getStateUpdateFunction() {
            return stateUpdateFunction;
        }

        public void setStateUpdateFunction(String stateUpdateFunction) {
            this.stateUpdateFunction = stateUpdateFunction;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public void setGroupFields(List<String> groupFields) {
            this.groupFields = groupFields;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public void setFailFast(Boolean failFast) {
            this.failFast = failFast;
        }

    }

    private class Mapping implements Serializable {

        private String function;
        private String outputField;
        private JavaScriptTransform.DataType outputType;

        public String getFunction() {
            return function;
        }

        public void setFunction(String function) {
            this.function = function;
        }

        public String getOutputField() {
            return outputField;
        }

        public void setOutputField(String outputField) {
            this.outputField = outputField;
        }

        public JavaScriptTransform.DataType getOutputType() {
            return outputType;
        }

        public void setOutputType(JavaScriptTransform.DataType outputType) {
            this.outputType = outputType;
        }

    }

    private enum DataType implements Serializable {
        bool,
        string,
        bytes,
        int32,
        int64,
        float32,
        float64,
        time,
        date,
        timestamp
    }

    public String getName() { return "javascript"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(List<FCollection<?>> inputs, TransformConfig config) {

        final JavaScriptTransformParameters parameters = new Gson().fromJson(config.getParameters(), JavaScriptTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final String nameError = name + OUTPUT_SUFFIX_FAILURES;
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Schema outputSchema = createOutputAvroSchema(input.getAvroSchema(), parameters.getMappings());
                    final Transform<GenericRecord,String,Schema> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            AvroSchemaUtil::convertSchema,
                            RecordToMapConverter::convert,
                            AvroSchemaUtil::getAsString,
                            (Schema s, GenericRecord r, List<Mapping> mappings, Map<String, Object> values) -> {
                                final GenericRecordBuilder builder = AvroSchemaUtil.copy(r, s);
                                for(var mapping : mappings) {
                                    if(!values.containsKey(mapping.getOutputField())) {
                                        builder.set(mapping.getOutputField(), null);
                                        continue;
                                    }
                                    switch (mapping.getOutputType()) {
                                        case bool:
                                        case string:
                                        case int32:
                                        case int64:
                                        case float32:
                                        case float64: {
                                            builder.set(mapping.getOutputField(), values.get(mapping.getOutputField()));
                                            break;
                                        }
                                        case bytes: {
                                            final byte[] bytes = (byte[])values.get(mapping.getOutputField());
                                            builder.set(mapping.getOutputField(), ByteBuffer.wrap(bytes));
                                            break;
                                        }
                                        case time: {
                                            builder.set(mapping.getOutputField(),
                                                    Long.valueOf(((LocalTime)values.get(mapping.getOutputField())).toNanoOfDay()/1000_000).intValue());
                                            break;
                                        }
                                        case date: {
                                            builder.set(mapping.getOutputField(),
                                                    Long.valueOf(((LocalDate)values.get(mapping.getOutputField())).toEpochDay()).intValue());
                                            break;
                                        }
                                        case timestamp: {
                                            final Instant instant = (Instant)values.get(mapping.getOutputField());
                                            builder.set(mapping.getOutputField(),
                                                    instant.getEpochSecond() * 1000_1000 + instant.getNano() / 1000);
                                            break;
                                        }
                                    }
                                }
                                return builder.build();
                            });
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(name, transform);
                    final PCollection<GenericRecord> output = tuple.get(transform.tagMain).setCoder(AvroCoder.of(outputSchema));
                    final PCollection<GenericRecord> error  = tuple.get(transform.tagError).setCoder(AvroCoder.of(input.getAvroSchema()));
                    results.put(name, FCollection.of(name, output, com.mercari.solution.module.DataType.AVRO, outputSchema));
                    results.put(nameError, FCollection.of(nameError, error, com.mercari.solution.module.DataType.AVRO, input.getAvroSchema()));
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final org.apache.beam.sdk.schemas.Schema outputSchema = createOutputRowSchema(input.getSchema(), parameters.getMappings());
                    final Transform<Row,org.apache.beam.sdk.schemas.Schema,org.apache.beam.sdk.schemas.Schema> transform = new Transform<>(
                            parameters,
                            outputSchema,
                            s -> s,
                            RowToMapConverter::convert,
                            RowSchemaUtil::getAsString,
                            (org.apache.beam.sdk.schemas.Schema s, Row r, List<Mapping> mappings, Map<String, Object> values) -> {
                                var builder = RowSchemaUtil.toBuilder(s, r);
                                for(var mapping : mappings) {
                                    if(!values.containsKey(mapping.getOutputField())) {
                                        builder.withFieldValue(mapping.getOutputField(), null);
                                        continue;
                                    }
                                    switch (mapping.getOutputType()) {
                                        case bool:
                                        case string:
                                        case bytes:
                                        case int32:
                                        case int64:
                                        case float32:
                                        case float64:
                                        case time:
                                        case date: {
                                            builder.withFieldValue(mapping.getOutputField(), values.get(mapping.getOutputField()));
                                            break;
                                        }
                                        case timestamp: {
                                            final Instant instant = (Instant)values.get(mapping.getOutputField());
                                            builder.withFieldValue(mapping.getOutputField(), DateTimeUtil.toJodaInstant(instant));
                                            break;
                                        }
                                    }
                                }
                                return builder.build();
                            });
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(name, transform);
                    final PCollection<Row> output = tuple.get(transform.tagMain).setCoder(RowCoder.of(outputSchema));
                    final PCollection<Row> error  = tuple.get(transform.tagError).setCoder(RowCoder.of(input.getSchema()));
                    results.put(name, FCollection.of(name, output, com.mercari.solution.module.DataType.ROW, outputSchema));
                    results.put(nameError, FCollection.of(nameError, error, com.mercari.solution.module.DataType.ROW, input.getSchema()));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Type outputType = createOutputStructType(input.getSpannerType(), parameters.getMappings());
                    final Transform<Struct,Type,Type> transform = new Transform<>(
                            parameters,
                            outputType,
                            s -> s,
                            StructToMapConverter::convert,
                            StructSchemaUtil::getAsString,
                            (Type s, Struct r, List<Mapping> mappings, Map<String, Object> values) -> {
                                var builder = StructSchemaUtil.toBuilder(s, r);
                                for(var mapping : mappings) {
                                    final Object value = values.get(mapping.getOutputField());
                                    switch (mapping.getOutputType()) {
                                        case bool:
                                            builder.set(mapping.getOutputField()).to((Boolean)value);
                                            break;
                                        case string:
                                            builder.set(mapping.getOutputField()).to((String)value);
                                            break;
                                        case bytes: {
                                            final byte[] bytes = (byte[])value;
                                            if(bytes == null) {
                                                builder.set(mapping.getOutputField()).to((ByteArray)null);
                                            } else {
                                                builder.set(mapping.getOutputField()).to(ByteArray.copyFrom(bytes));
                                            }
                                            break;
                                        }
                                        case int32:
                                            builder.set(mapping.getOutputField()).to((Integer)value);
                                            break;
                                        case int64:
                                            builder.set(mapping.getOutputField()).to((Long)value);
                                            break;
                                        case float32:
                                            builder.set(mapping.getOutputField()).to((Float)value);
                                            break;
                                        case float64:
                                            builder.set(mapping.getOutputField()).to((Double)value);
                                            break;
                                        case time: {
                                            final LocalTime localTime = (LocalTime) value;
                                            if(localTime == null) {
                                                builder.set(mapping.getOutputField()).to((String) null);
                                            } else {
                                                builder.set(mapping.getOutputField()).to(localTime.toString());
                                            }
                                            break;
                                        }
                                        case date: {
                                            final LocalDate localDate = (LocalDate) value;
                                            if(localDate == null) {
                                                builder.set(mapping.getOutputField()).to((Date)null);
                                            } else {
                                                builder.set(mapping.getOutputField()).to(Date
                                                        .fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
                                            }
                                            break;
                                        }
                                        case timestamp: {
                                            final Instant instant = (Instant)values.get(mapping.getOutputField());
                                            if(instant == null) {
                                                builder.set(mapping.getOutputField()).to((Timestamp)null);
                                            } else {
                                                builder.set(mapping.getOutputField()).to(Timestamp
                                                        .ofTimeSecondsAndNanos(instant.getEpochSecond(), instant.getNano()));
                                            }
                                            break;
                                        }
                                    }
                                }
                                return builder.build();
                            });
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(name, transform);
                    final PCollection<Struct> output = tuple.get(transform.tagMain).setCoder(inputCollection.getCollection().getCoder());
                    final PCollection<Struct> error  = tuple.get(transform.tagError).setCoder(inputCollection.getCollection().getCoder());
                    results.put(name, FCollection.of(name, output, com.mercari.solution.module.DataType.STRUCT, outputType));
                    results.put(nameError, FCollection.of(nameError, error, com.mercari.solution.module.DataType.STRUCT, input.getSpannerType()));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final org.apache.beam.sdk.schemas.Schema outputSchema = createOutputRowSchema(input.getSchema(), parameters.getMappings());
                    final Transform<Entity,org.apache.beam.sdk.schemas.Schema,org.apache.beam.sdk.schemas.Schema> transform = new Transform<>(
                            parameters,
                            outputSchema,
                            s -> s,
                            EntityToMapConverter::convert,
                            EntitySchemaUtil::getAsString,
                            (org.apache.beam.sdk.schemas.Schema s, Entity r, List<Mapping> mappings, Map<String, Object> values) -> {
                                var builder = EntitySchemaUtil.toBuilder(s, r);
                                for(var mapping : mappings) {
                                    final Object value = values.getOrDefault(mapping.getOutputField(), null);
                                    if(value == null) {
                                        builder.putProperties(mapping.getOutputField(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
                                        continue;
                                    }
                                    switch (mapping.getOutputType()) {
                                        case bool:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setBooleanValue((Boolean) value).build());
                                            break;
                                        case string:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setStringValue((String) value).build());
                                            break;
                                        case bytes:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setBlobValue(ByteString.copyFrom((byte[]) value)).build());
                                            break;
                                        case int32:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setIntegerValue((Integer) value).build());
                                            break;
                                        case int64:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setIntegerValue((Long) value).build());
                                            break;
                                        case float32:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setDoubleValue((Float) value).build());
                                            break;
                                        case float64:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setDoubleValue((Double) value).build());
                                            break;
                                        case time:
                                        case date:
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder().setStringValue(value.toString()).build());
                                            break;
                                        case timestamp: {
                                            final Instant instant = (Instant) value;
                                            builder.putProperties(mapping.getOutputField(), Value.newBuilder()
                                                    .setTimestampValue(com.google.protobuf.Timestamp.newBuilder()
                                                            .setSeconds(instant.getEpochSecond())
                                                            .setNanos(instant.getNano())
                                                            .build())
                                                    .build());
                                            break;
                                        }
                                    }
                                }
                                return builder.build();
                            });
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(name, transform);
                    final PCollection<Entity> output = tuple.get(transform.tagMain).setCoder(inputCollection.getCollection().getCoder());
                    final PCollection<Entity> error  = tuple.get(transform.tagError).setCoder(inputCollection.getCollection().getCoder());
                    results.put(name, FCollection.of(name, output, com.mercari.solution.module.DataType.ENTITY, outputSchema));
                    results.put(nameError, FCollection.of(nameError, error, com.mercari.solution.module.DataType.ENTITY, input.getSchema()));
                    break;
                }
                default: {
                    throw new IllegalStateException("");
                }
            }
        }

        return results;
    }

    private static Schema createOutputAvroSchema(final Schema inputSchema, final List<Mapping> mappings) {
        SchemaBuilder.FieldAssembler<Schema> schemaBuilder = AvroSchemaUtil.toBuilder(inputSchema);
        for(final Mapping mapping : mappings) {
            switch (mapping.getOutputType()) {
                case bool:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault();
                    break;
                case string:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
                    break;
                case bytes:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_BYTES).noDefault();
                    break;
                case int32:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_INT).noDefault();
                    break;
                case int64:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_LONG).noDefault();
                    break;
                case float32:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault();
                    break;
                case float64:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault();
                    break;
                case time:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MILLI_TYPE).noDefault();
                    break;
                case date:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault();
                    break;
                case timestamp:
                    schemaBuilder = schemaBuilder.name(mapping.getOutputField()).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault();
                    break;
                default: {
                    throw new IllegalStateException("JavaScript module not support outputType: " + mapping.getOutputType());
                }
            }
        }
        return schemaBuilder.endRecord();
    }

    private static org.apache.beam.sdk.schemas.Schema createOutputRowSchema(final org.apache.beam.sdk.schemas.Schema inputSchema, final List<Mapping> mappings) {
        org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder = RowSchemaUtil.toBuilder(inputSchema);
        for(final Mapping mapping : mappings) {
            switch (mapping.getOutputType()) {
                case bool:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN);
                    break;
                case string:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.STRING);
                    break;
                case bytes:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.BYTES);
                    break;
                case int32:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.INT32);
                    break;
                case int64:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.INT64);
                    break;
                case float32:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT);
                    break;
                case float64:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE);
                    break;
                case time:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.NULLABLE_TIME.getLogicalType()));
                    break;
                case date:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.NULLABLE_DATE.getLogicalType()));
                    break;
                case timestamp:
                    schemaBuilder = schemaBuilder.addField(mapping.getOutputField(), org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME);
                    break;
                default: {
                    throw new IllegalStateException("JavaScript module not support outputType: " + mapping.getOutputType());
                }
            }
        }
        return schemaBuilder.build();
    }

    private static Type createOutputStructType(final Type inputType, final List<Mapping> mappings) {
        final List<Type.StructField> fields = new ArrayList<>();
        fields.addAll(inputType.getStructFields());
        for(final Mapping mapping : mappings) {
            final Type fieldType;
            switch (mapping.getOutputType()) {
                case bool:
                    fieldType = Type.bool();
                    break;
                case time:
                case string:
                    fieldType = Type.string();
                    break;
                case bytes:
                    fieldType = Type.bytes();
                    break;
                case int32:
                case int64:
                    fieldType = Type.int64();
                    break;
                case float32:
                case float64:
                    fieldType = Type.float64();
                    break;
                case date:
                    fieldType = Type.date();
                    break;
                case timestamp:
                    fieldType = Type.timestamp();
                    break;
                default:
                    throw new IllegalStateException("JavaScript module not support outputType: " + mapping.getOutputType());
            }
            fields.add(Type.StructField.of(mapping.getOutputField(), fieldType));
        }
        return Type.struct(fields);
    }

    private static void validateParameters(final JavaScriptTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BarTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getScript() == null) {
            errorMessages.add("JavaScriptTransform config parameters must contain script parameter.");
        }
        if(parameters.getMappings() == null || parameters.getMappings().size() == 0) {
            errorMessages.add("JavaScriptTransform config parameters must contain mappings parameter.");
        } else {
            for(Mapping mapping : parameters.getMappings()) {
                if(mapping.getFunction() == null) {
                    errorMessages.add("JavaScriptTransform mapping parameter requires function parameter.");
                }
                if(mapping.getOutputField() == null) {
                    errorMessages.add("JavaScriptTransform mapping parameter requires outputField parameter.");
                }
                if(mapping.getOutputType() == null) {
                    errorMessages.add("JavaScriptTransform mapping parameter requires outputType parameter.");
                }
            }
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(final JavaScriptTransformParameters parameters) {
        if(parameters.getGroupFields() == null) {
            parameters.setGroupFields(new ArrayList<>());
        }
        if(parameters.getFailFast() == null) {
            parameters.setFailFast(true);
        }
    }

    public static class Transform<ElementT,InputSchemaT,RuntimeSchemaT> extends PTransform<PCollection<ElementT>, PCollectionTuple> {

        public final TupleTag<ElementT> tagMain  = new TupleTag<>(){};
        public final TupleTag<ElementT> tagError = new TupleTag<>(){};

        private static final Logger LOG = LoggerFactory.getLogger(Transform.class);

        private final JavaScriptTransformParameters parameters;
        private final InputSchemaT inputSchema;
        private final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final MapConverter<ElementT> mapConverter;
        private final StringGetter<ElementT> stringGetter;
        private final FieldSetter<ElementT, RuntimeSchemaT> fieldSetter;


        private Transform(final JavaScriptTransformParameters parameters,
                          final InputSchemaT inputSchema,
                          final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                          final MapConverter<ElementT> mapConverter,
                          final StringGetter<ElementT> stringGetter,
                          final FieldSetter<ElementT, RuntimeSchemaT> fieldSetter) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.mapConverter = mapConverter;
            this.stringGetter = stringGetter;
            this.fieldSetter = fieldSetter;
        }

        @Override
        public PCollectionTuple expand(PCollection<ElementT> input) {

            final List<String> groupFields = parameters.getGroupFields();

            final PCollection<KV<String,ElementT>> withKey;
            if(parameters.getGroupFields().size() > 0) {
                withKey = input.apply("WithGroupKey", WithKeys.of((ElementT t) -> {
                    final StringBuilder sb = new StringBuilder();
                    for(String fieldName : groupFields) {
                        final String fieldValue = stringGetter.getAsString(t, fieldName);
                        sb.append(fieldValue == null ? "" : fieldValue);
                        sb.append("#");
                    }
                    if(sb.length() > 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    return sb.toString();
                })).setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));
            } else {
                withKey = input
                        .apply("WithGroupKey", WithKeys.of(""))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));
            }

            final PCollectionTuple tuple;
            if(parameters.getStateUpdateFunction() != null) {
                if(OptionUtil.isStreaming(input.getPipeline().getOptions())) {
                    tuple = withKey.apply("ExecuteScript", ParDo
                            .of(new ScriptStatefulStreamingDoFn<>(
                                    parameters.getScript(), parameters.getMappings(), parameters.getStateUpdateFunction(),
                                    schemaConverter, mapConverter, fieldSetter,
                                    inputSchema, parameters.getFailFast(), tagError))
                            .withOutputTags(tagMain, TupleTagList.of(tagError)));
                } else {
                    tuple = withKey.apply("ExecuteScript", ParDo
                            .of(new ScriptStatefulBatchDoFn<>(
                                    parameters.getScript(), parameters.getMappings(), parameters.getStateUpdateFunction(),
                                    schemaConverter, mapConverter, fieldSetter,
                                    inputSchema, parameters.getFailFast(), tagError))
                            .withOutputTags(tagMain, TupleTagList.of(tagError)));
                }
            } else {
                tuple = withKey.apply("ExecuteScript", ParDo
                        .of(new ScriptStatelessDoFn<>(
                                parameters.getScript(), parameters.getMappings(), parameters.getStateUpdateFunction(),
                                schemaConverter, mapConverter, fieldSetter,
                                inputSchema, parameters.getFailFast(), tagError))
                        .withOutputTags(tagMain, TupleTagList.of(tagError)));
            }

            return tuple;
        }

        private static abstract class ScriptDoFn<T,InputSchemaT,RuntimeSchemaT> extends DoFn<KV<String, T>, T> {

            static final String STATEID_STATES = "states";

            private final String script;
            private final List<Mapping> mappings;
            private final String stateUpdateFunction;
            private final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final MapConverter<T> mapConverter;
            private final FieldSetter<T, RuntimeSchemaT> fieldSetter;
            private final InputSchemaT inputSchema;

            private final Boolean failFast;
            private final TupleTag<T> tagError;

            private transient Invocable invocable;
            private transient RuntimeSchemaT runtimeSchema;

            ScriptDoFn(final String script,
                       final List<Mapping> mappings,
                       final String stateUpdateFunction,
                       final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                       final MapConverter<T> mapConverter,
                       final FieldSetter<T, RuntimeSchemaT> fieldSetter,
                       final InputSchemaT inputSchema,
                       final Boolean failFast,
                       final TupleTag<T> tagError) {

                this.script = script;
                this.mappings = mappings;
                this.stateUpdateFunction = stateUpdateFunction;

                this.schemaConverter = schemaConverter;
                this.mapConverter = mapConverter;
                this.fieldSetter = fieldSetter;
                this.inputSchema = inputSchema;

                this.failFast = failFast;
                this.tagError = tagError;
            }

            void setup() throws ScriptException {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
                final String scriptText;
                if(script.startsWith("gs://")) {
                    scriptText = StorageUtil.readString(script);
                } else {
                    scriptText = script;
                }
                final ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
                engine.eval(scriptText);
                this.invocable = (Invocable) engine;
            }

            void processElement(final ProcessContext c,
                                final ValueState<Map<String, Double>> valueState) throws NoSuchMethodException {

                final T input = c.element().getValue();
                final Map<String, Object> mapInput = mapConverter.convert(input);
                Map<String, Double> states;
                if(stateUpdateFunction != null) {
                    states = Optional.ofNullable(valueState.read()).orElse(new HashMap<>());
                } else {
                    states = null;
                }

                // Execute scripts
                final Map<String, Object> outputs = new HashMap<>();
                for(final Mapping mapping : mappings) {
                    try {
                        // process output
                        final Object result;
                        if(stateUpdateFunction != null) {
                            result = this.invocable.invokeFunction(mapping.getFunction(), mapInput, states);
                        } else {
                            result = this.invocable.invokeFunction(mapping.getFunction(), mapInput);
                        }

                        // insert outputs
                        if (result == null) {
                            outputs.put(mapping.getOutputField(), null);
                            mapInput.put(mapping.getOutputField(), null);
                        } else {
                            final Object output = convert(mapping, result);
                            outputs.put(mapping.getOutputField(), output);
                            mapInput.put(mapping.getOutputField(), output);
                        }
                    } catch (NoSuchMethodException e) {
                        final String message = "Not found function: " + mapping.getFunction() + ", in script: \n" + script;
                        throw new NoSuchMethodException(message);
                    } catch (ScriptException e) {
                        final String message = "Script error for function: " + mapping.getFunction();
                        if(failFast) {
                            throw new IllegalStateException(message, e);
                        }
                        LOG.error(message + ", cause: " + e.getMessage());
                        c.output(tagError, input);
                    } catch (Exception e) {
                        final String message = "Script internal error for function: " + mapping.getFunction();
                        if(failFast) {
                            throw new IllegalStateException(message, e);
                        }
                        LOG.error(message + ", cause: " + e.getMessage());
                        c.output(tagError, input);
                    }
                }

                // Update states
                if(stateUpdateFunction != null) {
                    try {
                        final Object updatedStates = this.invocable.invokeFunction(stateUpdateFunction, mapInput, states);
                        if(updatedStates == null) {
                            states.clear();
                        } else if (updatedStates instanceof Map) {
                            final Map stateUpdateMap = (Map) updatedStates;
                            final Map<String, Double> updatedDoubleStates = convertDoubleMap(stateUpdateMap);
                            states.putAll(updatedDoubleStates);
                        } else {
                            throw new IllegalStateException("JavaScript module stateFunction must return type Map<String,Double>. not: " + updatedStates.getClass());
                        }

                    } catch (NoSuchMethodException e) {
                        final String message = "Not found stateFunction: " + stateUpdateFunction + ", in script: \n" + script;
                        throw new NoSuchMethodException(message);
                    } catch (ScriptException e) {
                        final String message = "Script error for stateFunction: " + stateUpdateFunction;
                        if (failFast) {
                            throw new IllegalStateException(message, e);
                        }
                        LOG.error(message + ", cause: " + e.getMessage());
                        c.output(tagError, input);
                    } catch (Exception e) {
                        final String message = "Script internal error for stateFunction: " + stateUpdateFunction;
                        if (failFast) {
                            throw new IllegalStateException(message, e);
                        }
                        LOG.error(message + ", cause: " + e.getMessage());
                        c.output(tagError, input);
                    }
                    valueState.write(states);
                }

                final T output = fieldSetter.setValues(runtimeSchema, input, mappings, outputs);
                c.output(output);
            }

            private Object convert(final Mapping mapping, final Object value) {
                if(value == null) {
                    return null;
                }
                switch (mapping.getOutputType()) {
                    case bool: {
                        if(value instanceof Boolean) {
                            return value;
                        } else if(value instanceof String) {
                            Boolean.valueOf((String)value);
                        } else if(value instanceof Number) {
                            return ((Number) value).doubleValue() > 0;
                        } else {
                            throw new IllegalStateException("JS response not supported bool type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case string: {
                        if(value instanceof String) {
                            return value;
                        } else {
                            throw new IllegalStateException("JS response not supported string type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case bytes: {
                        if(value instanceof String) {
                            return Base64.getDecoder().decode((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported bytes type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case int32: {
                        if(value instanceof Integer) {
                            return value;
                        } else if(value instanceof Long) {
                            return ((Long)value).intValue();
                        } else if(value instanceof Float) {
                            return ((Float)value).intValue();
                        } else if(value instanceof Double) {
                            return ((Double)value).intValue();
                        } else if(value instanceof String) {
                            return Integer.valueOf((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported int32 type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case int64: {
                        if(value instanceof Long) {
                            return value;
                        } else if(value instanceof Integer) {
                            return ((Integer)value).longValue();
                        } else if(value instanceof Float) {
                            return ((Float)value).longValue();
                        } else if(value instanceof Double) {
                            return ((Double)value).longValue();
                        } else if(value instanceof String) {
                            return Long.valueOf((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported int64 type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case float32: {
                        if(value instanceof Float) {
                            return value;
                        } else if(value instanceof Double) {
                            return ((Double)value).floatValue();
                        } else if(value instanceof Integer) {
                            return ((Integer)value).floatValue();
                        } else if(value instanceof Long) {
                            return ((Long)value).floatValue();
                        } else if(value instanceof String) {
                            return Float.valueOf((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported float32 type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case float64: {
                        if(value instanceof Double) {
                            return value;
                        } else if(value instanceof Float) {
                            return ((Float)value).doubleValue();
                        } else if(value instanceof Integer) {
                            return ((Integer)value).doubleValue();
                        } else if(value instanceof Long) {
                            return ((Long)value).doubleValue();
                        } else if(value instanceof String) {
                            return Double.valueOf((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported float64 type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case time: {
                        if(value instanceof String) {
                            return DateTimeUtil.toLocalTime((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported time type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case date: {
                        if(value instanceof String) {
                            return DateTimeUtil.toLocalDate((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported date type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    case timestamp: {
                        if(value instanceof String) {
                            return DateTimeUtil.toInstant((String)value);
                        } else {
                            throw new IllegalStateException("JS response not supported timestamp type outputField: "
                                    + mapping.getOutputField() + ", value: " + value);
                        }
                    }
                    default: {
                        throw new IllegalStateException("");
                    }
                }
            }

            private static Map<String, Double> convertDoubleMap(final Map<String, ?> input) {
                return input.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    if(e.getValue() instanceof Double) {
                        if(Double.isNaN((Double)e.getValue())) {
                            return null;
                        }
                        return (Double)e.getValue();
                    } else if(e.getValue() instanceof Integer) {
                        return Double.valueOf((Integer)e.getValue());
                    } else if(e.getValue() instanceof String) {
                        return Double.valueOf((String)e.getValue());
                    } else {
                        throw new IllegalStateException();
                    }
                }));
            }

        }

        private static class ScriptStatefulBatchDoFn<T,InputSchemaT,RuntimeSchemaT> extends ScriptDoFn<T,InputSchemaT,RuntimeSchemaT> {

            @StateId(STATEID_STATES)
            private final StateSpec<ValueState<Map<String, Double>>> stateSpec;

            ScriptStatefulBatchDoFn(final String script,
                                    final List<Mapping> mappings,
                                    final String stateUpdateFunction,
                                    final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                                    final MapConverter<T> mapConverter,
                                    final FieldSetter<T, RuntimeSchemaT> fieldSetter,
                                    final InputSchemaT inputSchema,
                                    final Boolean failFast,
                                    final TupleTag<T> tagError) {

                super(script, mappings, stateUpdateFunction, schemaConverter, mapConverter, fieldSetter, inputSchema, failFast, tagError);
                this.stateSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
            }

            @Setup
            public void setup() throws ScriptException {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_STATES) ValueState<Map<String,Double>> state)
                    throws NoSuchMethodException {

                super.processElement(c, state);
            }

        }

        private static class ScriptStatefulStreamingDoFn<T,InputSchemaT,RuntimeSchemaT> extends ScriptDoFn<T,InputSchemaT,RuntimeSchemaT> {

            @StateId(STATEID_STATES)
            private final StateSpec<ValueState<Map<String, Double>>> stateSpec;

            ScriptStatefulStreamingDoFn(final String script,
                                        final List<Mapping> mappings,
                                        final String stateUpdateFunction,
                                        final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                                        final MapConverter<T> mapConverter,
                                        final FieldSetter<T, RuntimeSchemaT> fieldSetter,
                                        final InputSchemaT inputSchema,
                                        final Boolean failFast,
                                        final TupleTag<T> tagError) {

                super(script, mappings, stateUpdateFunction, schemaConverter, mapConverter, fieldSetter, inputSchema, failFast, tagError);
                this.stateSpec = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
            }

            @Setup
            public void setup() throws ScriptException {
                super.setup();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_STATES) ValueState<Map<String,Double>> state)
                    throws NoSuchMethodException {

                super.processElement(c, state);
            }

        }

        private static class ScriptStatelessDoFn<T,InputSchemaT,RuntimeSchemaT> extends ScriptDoFn<T,InputSchemaT,RuntimeSchemaT> {

            ScriptStatelessDoFn(final String script,
                                final List<Mapping> mappings,
                                final String stateUpdateFunction,
                                final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                                final MapConverter<T> mapConverter,
                                final FieldSetter<T, RuntimeSchemaT> fieldSetter,
                                final InputSchemaT inputSchema,
                                final Boolean failFast,
                                final TupleTag<T> tagError) {

                super(script, mappings, stateUpdateFunction, schemaConverter, mapConverter, fieldSetter, inputSchema, failFast, tagError);
            }

            @Setup
            public void setup() throws ScriptException {
                super.setup();
            }

            @ProcessElement
            public void processElement(final ProcessContext c)
                    throws NoSuchMethodException {

                super.processElement(c, null);
            }

        }

    }


    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(InputSchemaT schema);
    }

    private interface MapConverter<T> extends Serializable {
        Map<String, Object> convert(T element);
    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(T element, String field);
    }

    private interface FieldSetter<T, SchemaT> extends Serializable {
        T setValues(SchemaT schema, T element, List<Mapping> mappings, Map<String, Object> values);
    }

}
