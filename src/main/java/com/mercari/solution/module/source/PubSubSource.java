package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.converter.JsonToRecordConverter;
import com.mercari.solution.util.converter.JsonToRowConverter;
import com.mercari.solution.util.converter.PubSubToRecordConverter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class PubSubSource implements SourceModule {

    private class PubSubSourceParameters {

        private String topic;
        private String subscription;
        private Format format;
        private String idAttribute;
        private String eventTimeField;
        private OutputType outputType;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getSubscription() {
            return subscription;
        }

        public void setSubscription(String subscription) {
            this.subscription = subscription;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public void setIdAttribute(String idAttribute) {
            this.idAttribute = idAttribute;
        }

        public String getEventTimeField() {
            return eventTimeField;
        }

        public void setEventTimeField(String eventTimeField) {
            this.eventTimeField = eventTimeField;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public void setOutputType(OutputType outputType) {
            this.outputType = outputType;
        }
    }

    public String getName() { return "pubsub"; }

    private enum Format {
        avro,
        json,
        message
    }

    private enum OutputType {
        row,
        avro
    }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), PubSubSource.stream(begin, config));
    }

    public static FCollection<?> stream(final PBegin begin, final SourceConfig config) {

        final PubSubSourceParameters parameters = new Gson().fromJson(config.getParameters(), PubSubSourceParameters.class);
        validateParameters(begin, parameters);
        setDefaultParameters(parameters);

        switch (parameters.getFormat()) {
            case avro: {
                final Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
            }
            case json: {
                switch (parameters.getOutputType()) {
                    case avro: {
                        final Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                        final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                    }
                    case row: {
                        final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(config.getSchema());
                        final PubSubStream<Row> stream = new PubSubStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.ROW, rowSchema);
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
            case message: {
                final Schema avroSchema = PubSubToRecordConverter.createMessageSchema();
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
            }
            default:
                throw new IllegalStateException();
        }

    }

    private static void validateParameters(final PBegin begin, final PubSubSourceParameters parameters) {

        if(!begin.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming()) {
            throw new IllegalArgumentException("PubSub source module only support streaming mode.");
        }

        if(parameters == null) {
            throw new IllegalArgumentException("PubSub source module parameters must not be empty!");
        }

        // check required parameters filled
        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getTopic() == null && parameters.getSubscription() == null) {
            errorMessages.add("PubSub source module parameter must contain topic or subscription");
        }
        if(parameters.getFormat() == null) {
            errorMessages.add("PubSub source module parameter must contain format");
        }
        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
        }
    }

    private static void setDefaultParameters(final PubSubSourceParameters parameters) {
        if(parameters.getOutputType() == null) {
            switch (parameters.getFormat()) {
                case avro:
                case message: {
                    parameters.setOutputType(OutputType.avro);
                    break;
                }
                case json: {
                    parameters.setOutputType(OutputType.row);
                    break;
                }
                default:
                    parameters.setOutputType(OutputType.row);
                    break;
            }
        }
    }

    public static class PubSubStream<T> extends PTransform<PBegin, PCollection<T>> {

        private final SourceConfig config;
        private final PubSubSourceParameters parameters;

        private PubSubStream(final SourceConfig config, final PubSubSourceParameters parameters) {
            this.config = config;
            this.parameters = parameters;
        }

        public PCollection<T> expand(final PBegin begin) {

            PubsubIO.Read read;
            switch (parameters.getFormat()) {
                case avro: {
                    read = PubsubIO.readAvroGenericRecords(SourceConfig.convertAvroSchema(config.getSchema()));
                    break;
                }
                case json: {
                    read = PubsubIO.readMessagesWithAttributesAndMessageId();
                    break;
                }
                case message: {
                    read = PubsubIO.readMessagesWithAttributesAndMessageId();
                    break;
                }
                default:
                    throw new IllegalArgumentException("PubSubSource must be set format avro or message. " + parameters.getFormat());
            }

            if(parameters.getTopic() != null) {
                read = read.fromTopic(parameters.getTopic());
            } else if(parameters.getSubscription() != null) {
                read = read.fromSubscription(parameters.getSubscription());
            }

            if(parameters.getIdAttribute() != null) {
                read = read.withIdAttribute(parameters.getIdAttribute());
            }
            if(config.getTimestampAttribute() != null) {
                read = read.withTimestampAttribute(config.getTimestampAttribute());
            }

            // Deserialize pubsub messages
            final PCollection<T> messages;
            switch (parameters.getFormat()) {
                case avro: {
                    messages = (PCollection<T>) begin.apply("ReadPubSubRecord", (PubsubIO.Read<GenericRecord>) read);
                    break;
                }
                case json: {
                    final PCollection<PubsubMessage> pubsubMessages = begin
                            .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read);
                    switch (parameters.getOutputType()) {
                        case avro: {
                            Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                            messages = (PCollection<T>) pubsubMessages.apply("JsonToRecord", ParDo.of(new JsonToRecordDoFn(avroSchema.toString())))
                                    .setCoder(AvroCoder.of(avroSchema));
                            break;
                        }
                        case row: {
                            final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(config.getSchema());
                            messages = (PCollection<T>) pubsubMessages.apply("JsonToRow", ParDo.of(new JsonToRowDoFn(rowSchema)))
                                    .setCoder(RowCoder.of(rowSchema));
                            break;
                        }
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                }
                case message: {
                    messages = (PCollection<T>) begin
                            .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read)
                            .apply("MessageToRecord", ParDo.of(new MessageToRecordDoFn()))
                            .setCoder(AvroCoder.of(PubSubToRecordConverter.createMessageSchema()));
                    break;
                }
                default:
                    throw new IllegalArgumentException();
            }

            // Add event time Field
            if(parameters.getEventTimeField() == null) {
                return messages;
            }

            if(parameters.getFormat().equals(Format.avro)
                    || parameters.getFormat().equals(Format.message)
                    || (parameters.getFormat().equals(Format.json) && parameters.getOutputType().equals(OutputType.avro))) {

                final PCollection<GenericRecord> withEventTimeField = ((PCollection<GenericRecord>)messages)
                        .apply("WithEventTimeField", ParDo.of(new WithEventTimeFieldDoFn<>(
                                parameters.getEventTimeField(),
                                (GenericRecord r, Map<String, Object> values) -> {
                                    final GenericRecordBuilder builder = AvroSchemaUtil.copy(r, r.getSchema());
                                    for(var entry : values.entrySet()) {
                                        builder.set(entry.getKey(), entry.getValue());
                                    }
                                    return builder.build();
                                },
                                (Instant timestamp) -> timestamp.getMillis() * 1000L)));
                return (PCollection<T>) withEventTimeField;
            } else {
                final PCollection<Row> withEventTimeField = ((PCollection<Row>)messages)
                        .apply("WithEventTimeField", ParDo.of(new WithEventTimeFieldDoFn<>(
                                parameters.getEventTimeField(),
                                (Row r, Map<String, Object> values) -> {
                                    final Row.FieldValueBuilder builder = RowSchemaUtil.toBuilder(r);
                                    for(var entry : values.entrySet()) {
                                        builder.withFieldValue(entry.getKey(), entry.getValue());
                                    }
                                    return builder.build();
                                },
                                (Instant timestamp) -> timestamp)));
                return (PCollection<T>) withEventTimeField;
            }
        }

    }

    private static class JsonToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private final String schemaString;
        private transient Schema schema;

        JsonToRecordDoFn(final String schemaString) {
            this.schemaString = schemaString;
        }

        @Setup
        public void setup() {
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            c.output(JsonToRecordConverter.convert(schema, json));
        }

    }

    private static class JsonToRowDoFn extends DoFn<PubsubMessage, Row> {

        private final org.apache.beam.sdk.schemas.Schema schema;

        JsonToRowDoFn(final org.apache.beam.sdk.schemas.Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            c.output(JsonToRowConverter.convert(schema, json));
        }

    }

    private static class MessageToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private transient Schema schema;

        @Setup
        public void setup() {
            this.schema = PubSubToRecordConverter.createMessageSchema();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(PubSubToRecordConverter.convertMessage(schema, c.element()));
        }

    }

    private static class WithEventTimeFieldDoFn<T, TimestampT> extends DoFn<T, T> {

        private final String eventTimeField;
        private final FieldSetter<T> setter;
        private final TimestampConverter<TimestampT> timestampConverter;

        WithEventTimeFieldDoFn(final String eventTimeField,
                               final FieldSetter<T> setter,
                               final TimestampConverter<TimestampT> timestampConverter) {

            this.eventTimeField = eventTimeField;
            this.setter = setter;
            this.timestampConverter = timestampConverter;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final T input = c.element();
            final Map<String, Object> values = new HashMap<>();
            final TimestampT timestampValue = timestampConverter.convert(c.timestamp());
            values.put(eventTimeField, timestampValue);
            final T output = setter.merge(input, values);
            c.output(output);
        }

    }

    private interface FieldSetter<T> extends Serializable {
        T merge(final T base, final Map<String, Object> values);
    }

    private interface TimestampConverter<T> extends Serializable {
        T convert(final Instant timestamp);
    }

}
