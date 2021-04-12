package com.mercari.solution.module.transform;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;


public class EventTimeTransform implements TransformModule {

    private class EventTimeTransformParameters implements Serializable {

        private String eventtimeField;
        private Boolean into;
        private Long allowedTimestampSkewSeconds;

        public String getEventtimeField() {
            return eventtimeField;
        }

        public void setEventtimeField(String eventtimeField) {
            this.eventtimeField = eventtimeField;
        }

        public Boolean getInto() {
            return into;
        }

        public void setInto(Boolean into) {
            this.into = into;
        }

        public Long getAllowedTimestampSkewSeconds() {
            return allowedTimestampSkewSeconds;
        }

        public void setAllowedTimestampSkewSeconds(Long allowedTimestampSkewSeconds) {
            this.allowedTimestampSkewSeconds = allowedTimestampSkewSeconds;
        }

    }

    @Override
    public String getName() {
        return "eventtime";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return EventTimeTransform.transform(inputs, config);
    }

    static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final EventTimeTransformParameters parameters = new Gson().fromJson(config.getParameters(), EventTimeTransformParameters.class);

        validateParameters(parameters);
        setDefaultParameters(parameters);

        final String timestampField = parameters.getEventtimeField();
        final Map<String, FCollection<?>> results = new HashMap<>();
        for (final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Schema schema;
                    final PCollection<GenericRecord> output;
                    if(parameters.getInto()) {
                        schema = AvroSchemaUtil.toBuilder(inputCollection.getAvroSchema(), inputCollection.getAvroSchema().getNamespace(), null)
                                .name(timestampField).type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                                .endRecord();
                        output = inputCollection.getCollection()
                                .apply(name, ParDo.of(new WithEventTimeFieldDoFn<>(
                                        timestampField,
                                        (Schema s, GenericRecord record, String fieldName, Instant timestamp) -> AvroSchemaUtil.toBuilder(s, record)
                                                    .set(fieldName, timestamp.getMillis() * 1000L)
                                                    .build(),
                                        schema.toString(),
                                        AvroSchemaUtil::convertSchema)))
                                .setCoder(AvroCoder.of(schema));
                    } else {
                        schema = inputCollection.getAvroSchema();
                        if(parameters.getAllowedTimestampSkewSeconds() == null) {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .of(record -> AvroSchemaUtil.getTimestamp(record, timestampField)));
                        } else {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .<GenericRecord>of(record -> AvroSchemaUtil.getTimestamp(record, timestampField))
                                            .withAllowedTimestampSkew(Duration.standardSeconds(parameters.getAllowedTimestampSkewSeconds())));
                        }
                    }
                    results.put(name, FCollection.of(name, output, DataType.AVRO, schema));
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final org.apache.beam.sdk.schemas.Schema schema;
                    final PCollection<Row> output;
                    if(parameters.getInto()) {
                        schema = RowSchemaUtil.toBuilder(inputCollection.getSchema())
                                .addField(timestampField, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME)
                                .build();
                        output = inputCollection.getCollection()
                                .apply(name, ParDo.of(new WithEventTimeFieldDoFn<>(
                                        timestampField,
                                        (org.apache.beam.sdk.schemas.Schema s, Row row, String field, Instant timestamp) -> {
                                            final Row.FieldValueBuilder builder = RowSchemaUtil.toBuilder(s, row);
                                            builder.withFieldValue(field, timestamp);
                                            return builder.build();
                                        },
                                        schema,
                                        s -> s)))
                                .setCoder(RowCoder.of(schema));
                    } else {
                        schema = inputCollection.getSchema();
                        if(parameters.getAllowedTimestampSkewSeconds() == null) {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .of(row -> RowSchemaUtil.getTimestamp(row, timestampField, Instant.ofEpochSecond(0L))));
                        } else {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .<Row>of(row -> RowSchemaUtil.getTimestamp(row, timestampField, Instant.ofEpochSecond(0L)))
                                            .withAllowedTimestampSkew(Duration.standardSeconds(parameters.getAllowedTimestampSkewSeconds())));
                        }
                    }
                    results.put(name, FCollection.of(name, output, DataType.ROW, schema));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Type type;
                    final PCollection<Struct> output;
                    if(parameters.getInto()) {
                        type = StructSchemaUtil.addStructField(
                                inputCollection.getSpannerType(),
                                Arrays.asList(Type.StructField.of(timestampField, Type.timestamp())));
                        output = inputCollection.getCollection()
                                .apply(name, ParDo.of(new WithEventTimeFieldDoFn<>(
                                        timestampField,
                                        (Type t, Struct struct, String field, Instant timestamp) -> {
                                            final Struct.Builder builder = StructSchemaUtil.toBuilder(t, struct);
                                            if(timestamp.getMillis() < -9223372036855L) {
                                                builder.set(field).to(Timestamp.ofTimeMicroseconds(-9223372036855L));
                                            } else {
                                                builder.set(field).to(Timestamp.ofTimeMicroseconds(timestamp.getMillis() * 1000));
                                            }
                                            return builder.build();
                                        },
                                        type,
                                        t -> t)));
                    } else {
                        type = inputCollection.getSpannerType();
                        if(parameters.getAllowedTimestampSkewSeconds() == null) {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .of(struct -> StructSchemaUtil.getTimestamp(struct, timestampField, Instant.ofEpochSecond(0L))));
                        } else {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .<Struct>of(struct -> StructSchemaUtil.getTimestamp(struct, timestampField, Instant.ofEpochSecond(0L)))
                                            .withAllowedTimestampSkew(Duration.standardSeconds(parameters.getAllowedTimestampSkewSeconds())));
                        }
                    }
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, type));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final org.apache.beam.sdk.schemas.Schema schema;
                    final PCollection<Entity> output;
                    if(parameters.getInto()) {
                        schema = RowSchemaUtil.toBuilder(inputCollection.getSchema())
                                .addField(timestampField, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME)
                                .build();
                        output = inputCollection.getCollection()
                                .apply(name, ParDo.of(new WithEventTimeFieldDoFn<>(
                                        timestampField,
                                        (org.apache.beam.sdk.schemas.Schema s, Entity entity, String field, Instant timestamp) -> entity
                                                .toBuilder()
                                                .putProperties(field, Value.newBuilder()
                                                    .setTimestampValue(Timestamps.fromMillis(
                                                            timestamp.getMillis()/1000 < -62135596800L ? -62135596800L * 1000 : timestamp.getMillis())).build())
                                                    .build(),
                                        schema,
                                        s -> s)));
                    } else {
                        schema = inputCollection.getSchema();
                        if(parameters.getAllowedTimestampSkewSeconds() == null) {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .of(entity -> EntitySchemaUtil.getTimestamp(entity, timestampField, Instant.now())));
                        } else {
                            output = inputCollection.getCollection()
                                    .apply(name, WithTimestamps
                                            .<Entity>of(entity -> EntitySchemaUtil.getTimestamp(entity, timestampField, Instant.now()))
                                            .withAllowedTimestampSkew(Duration.standardSeconds(parameters.getAllowedTimestampSkewSeconds())));
                        }
                    }
                    results.put(name, FCollection.of(name, output, DataType.ENTITY, schema));
                    break;
                }
                default:
                    break;
            }
        }

        return results;
    }

    private static void validateParameters(final EventTimeTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("Eventtime transform module parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getEventtimeField() == null) {
            errorMessages.add("Eventtime transform module parameters must contain eventtimeField.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(EventTimeTransformParameters parameters) {
        if(parameters.getInto() == null) {
            parameters.setInto(true);
        }
    }

    private static class WithEventTimeFieldDoFn<T,InputSchemaT,RuntimeSchemaT> extends DoFn<T, T> {

        private final String eventTimeField;
        private final TimestampSetter<T,RuntimeSchemaT> setter;
        private final InputSchemaT inputSchema;
        private final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;

        private transient RuntimeSchemaT schema;

        WithEventTimeFieldDoFn(final String eventTimeField,
                               final TimestampSetter<T,RuntimeSchemaT> setter,
                               final InputSchemaT inputSchema,
                               final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter) {

            this.eventTimeField = eventTimeField;
            this.setter = setter;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
        }

        @Setup
        public void setup() {
            this.schema = schemaConverter.convert(inputSchema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final T input = c.element();
            final T output = setter.merge(schema, input, eventTimeField, c.timestamp());
            c.output(output);
        }

    }

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(final InputSchemaT schema);
    }

    private interface TimestampSetter<T,SchemaT> extends Serializable {
        T merge(final SchemaT schema, final T base, final String timestampField, final Instant timestamp);
    }

}
