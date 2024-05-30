package com.mercari.solution.module.source;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.model.Subscription;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.PubSubUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class PubSubSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

    private static class PubSubSourceParameters implements Serializable {

        private String topic;
        private String subscription;

        private Format format;
        private String idAttribute;
        private OutputType outputType;

        private String messageName;

        private Boolean validateUnnecessaryJsonField;

        public String getTopic() {
            return topic;
        }

        public String getSubscription() {
            return subscription;
        }

        public Format getFormat() {
            return format;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public String getMessageName() {
            return messageName;
        }

        public Boolean getValidateUnnecessaryJsonField() {
            return validateUnnecessaryJsonField;
        }

        private void validate(final String name, final PBegin begin) {

            if(!begin.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming()) {
                throw new IllegalArgumentException("PubSub source module only support streaming mode.");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(topic == null && subscription == null) {
                errorMessages.add("PubSub source module parameter must contain topic or subscription");
            }
            if(format == null) {
                errorMessages.add("PubSub source module parameter must contain format");
            } else {
                switch (format) {
                    case protobuf -> {
                        if(this.messageName == null) {
                            errorMessages.add("PubSub source module[" + name + "].parameters requires messageName if format is parquet");
                        }
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(outputType == null) {
                switch (format) {
                    case avro, message -> {
                        outputType = OutputType.avro;
                    }
                    case protobuf, json -> {
                        outputType = OutputType.row;
                    }
                    default -> outputType = OutputType.row;
                }
            }
            if(validateUnnecessaryJsonField == null) {
                validateUnnecessaryJsonField = false;
            }
        }

        public static PubSubSourceParameters of(final SourceConfig config, final PBegin begin) {
            final PubSubSourceParameters parameters = new Gson().fromJson(config.getParameters(), PubSubSourceParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("PubSub source module parameters must not be empty!");
            }
            parameters.validate(config.getName(), begin);
            parameters.setDefaults();
            return parameters;
        }

    }

    public String getName() { return "pubsub"; }

    private enum Format {
        avro,
        json,
        protobuf,
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

        final PubSubSourceParameters parameters = PubSubSourceParameters.of(config, begin);
        switch (parameters.getFormat()) {
            case avro -> {
                final Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
            }
            case json -> {
                switch (parameters.getOutputType()) {
                    case avro -> {
                        final Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                        final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                    }
                    case row -> {
                        final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(config.getSchema());
                        final PubSubStream<Row> stream = new PubSubStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.ROW, rowSchema);
                    }
                    default ->
                            throw new IllegalStateException("PubSub source module does not support outputType: " + parameters.getOutputType());
                }
            }
            case protobuf -> {
                final byte[] descriptorBytes = StorageUtil.readBytes(config.getSchema().getProtobufDescriptor());
                final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descriptorBytes);
                if(descriptors.isEmpty()) {
                    throw new IllegalArgumentException("protobuf descriptors must not be null for descriptor file: " + config.getSchema().getProtobufDescriptor());
                }
                if(!descriptors.containsKey(parameters.getMessageName())) {
                    throw new IllegalArgumentException("protobuf descriptors does not contains messageName: " + parameters.getMessageName() + " in descriptors: " + descriptors.keySet());
                }
                final Descriptors.Descriptor messageDescriptor = descriptors.get(parameters.getMessageName());
                switch (parameters.getOutputType()) {
                    case avro -> {
                        final Schema avroSchema = ProtoToRecordConverter.convertSchema(messageDescriptor);
                        final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                    }
                    case row -> {
                        final org.apache.beam.sdk.schemas.Schema rowSchema = ProtoToRowConverter.convertSchema(messageDescriptor);
                        final PubSubStream<Row> stream = new PubSubStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.ROW, rowSchema);
                    }
                    default ->
                            throw new IllegalStateException("PubSub source module does not support outputType: " + parameters.getOutputType());
                }
            }
            case message -> {
                final Schema avroSchema = PubSubToRecordConverter.createMessageSchema();
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
            }
            default ->
                    throw new IllegalStateException("PubSub source module does not support format: " + parameters.getFormat());
        }

    }


    public static class PubSubStream<T> extends PTransform<PBegin, PCollection<T>> {

        private final SourceConfig.InputSchema schema;
        private final String timestampAttribute;
        private final PubSubSourceParameters parameters;

        private PubSubStream(final SourceConfig config, final PubSubSourceParameters parameters) {
            this.schema = config.getSchema();
            this.timestampAttribute = config.getTimestampAttribute();
            this.parameters = parameters;
        }

        public PCollection<T> expand(final PBegin begin) {

            PubsubIO.Read read = PubsubIO.readMessagesWithAttributesAndMessageId();
            if (parameters.getTopic() != null) {
                read = read.fromTopic(parameters.getTopic());
            } else if (parameters.getSubscription() != null) {
                read = read.fromSubscription(parameters.getSubscription());
            }

            if (parameters.getIdAttribute() != null) {
                read = read.withIdAttribute(parameters.getIdAttribute());
            }
            if (timestampAttribute != null) {
                read = read.withTimestampAttribute(timestampAttribute);
            }

            final String deadletterTopic;
            final boolean sendDeadletter;
            if (parameters.getSubscription() == null) {
                deadletterTopic = null;
                sendDeadletter = false;
            } else {
                deadletterTopic = getDeadLetterTopic(parameters.getSubscription());;
                sendDeadletter = deadletterTopic != null;;
            }

            final TupleTag<PubsubMessage> failuresTag = new TupleTag<>() {};

            // Deserialize pubsub messages
            final PCollection<PubsubMessage> pubsubMessages = begin
                    .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read);

            final PCollection<T> messages;
            final PCollection<PubsubMessage> failures;
            switch (parameters.getFormat()) {
                case avro -> {
                    final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {
                    };
                    final Schema avroSchema = SourceConfig.convertAvroSchema(schema);
                    final PCollectionTuple tuple = pubsubMessages
                            .apply("AvroToRecord", ParDo
                                    .of(new AvroToRecordDoFn(sendDeadletter, avroSchema.toString(), failuresTag))
                                    .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                    messages = (PCollection<T>) tuple.get(outputAvroTag).setCoder(AvroCoder.of(avroSchema));
                    failures = tuple.get(failuresTag);
                }
                case json -> {
                    switch (parameters.getOutputType()) {
                        case avro -> {
                            final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {
                            };
                            final Schema avroSchema = SourceConfig.convertAvroSchema(schema);
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("JsonToRecord", ParDo
                                            .of(new JsonToRecordDoFn(sendDeadletter, avroSchema.toString(), failuresTag, parameters.getValidateUnnecessaryJsonField()))
                                            .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputAvroTag)
                                    .setCoder(AvroCoder.of(avroSchema));
                            failures = tuple.get(failuresTag);
                        }
                        case row -> {
                            final TupleTag<Row> outputRowTag = new TupleTag<>() {
                            };
                            final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(schema);
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("JsonToRow", ParDo
                                            .of(new JsonToRowDoFn(sendDeadletter, rowSchema, failuresTag, parameters.getValidateUnnecessaryJsonField()))
                                            .withOutputTags(outputRowTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputRowTag)
                                    .setCoder(RowCoder.of(rowSchema));
                            failures = tuple.get(failuresTag);
                        }
                        default -> throw new IllegalStateException();
                    }
                }
                case protobuf -> {
                    final Map<String, Descriptors.Descriptor> descriptors = SourceConfig.convertProtobufDescriptors(schema);
                    switch (parameters.getOutputType()) {
                        case avro: {
                            final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {
                            };
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("ProtobufToRecord", ParDo
                                            .of(new ProtoToRecordDoFn(sendDeadletter,
                                                    parameters.getMessageName(),
                                                    schema.getProtobufDescriptor(),
                                                    failuresTag))
                                            .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                            final Schema avroSchema = ProtoToRecordConverter.convertSchema(descriptors.get(parameters.getMessageName()));
                            messages = (PCollection<T>) tuple.get(outputAvroTag)
                                    .setCoder(AvroCoder.of(avroSchema));
                            failures = tuple.get(failuresTag);
                            break;
                        }
                        case row: {
                            final TupleTag<Row> outputRowTag = new TupleTag<>() {
                            };
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("ProtobufToRow", ParDo
                                            .of(new ProtoToRowDoFn(sendDeadletter, parameters.getMessageName(),
                                                    schema.getProtobufDescriptor(),
                                                    failuresTag))
                                            .withOutputTags(outputRowTag, TupleTagList.of(failuresTag)));
                            final org.apache.beam.sdk.schemas.Schema rowSchema = ProtoToRowConverter.convertSchema(descriptors.get(parameters.getMessageName()));
                            messages = (PCollection<T>) tuple.get(outputRowTag)
                                    .setCoder(RowCoder.of(rowSchema));
                            failures = tuple.get(failuresTag);
                            break;
                        }
                        default:
                            throw new IllegalStateException();
                    }
                }
                case message -> {
                    messages = (PCollection<T>) pubsubMessages
                            .apply("MessageToRecord", ParDo.of(new MessageToRecordDoFn()))
                            .setCoder(AvroCoder.of(PubSubToRecordConverter.createMessageSchema()));
                    failures = null;
                }
                default -> throw new IllegalArgumentException();
            }

            if (failures != null && deadletterTopic != null) {
                failures.apply("PublishDeadLetter", PubsubIO.writeMessages().to(deadletterTopic));
            }

            return messages;
        }

        private String getDeadLetterTopic(String subscriptionName) {
            try {
                final Subscription subscription = PubSubUtil.pubsub()
                        .projects()
                        .subscriptions()
                        .get(subscriptionName)
                        .execute();
                if(subscription.getDeadLetterPolicy() == null) {
                    return null;
                }
                return subscription.getDeadLetterPolicy().getDeadLetterTopic();
            } catch (GoogleJsonResponseException e) {
                if(e.getStatusCode() == 404) {
                    return null;
                } else if(e.getStatusCode() == 403) {
                    LOG.warn("dataflow worker does not have dead-letter topic access permission for subscription: " + subscriptionName);
                    return null;
                }
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }


    private static class AvroToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private final TupleTag<PubsubMessage> failuresTag;

        private final boolean sendDeadletter;
        private final String schemaString;

        private transient Schema schema;
        // https://beam.apache.org/documentation/programming-guide/#user-code-thread-compatibility
        private transient DatumReader<GenericRecord> datumReader;
        private transient BinaryDecoder decoder = null;

        AvroToRecordDoFn(final boolean sendDeadletter,
                         final String schemaString,
                         final TupleTag<PubsubMessage> failuresTag) {

            this.sendDeadletter = sendDeadletter;
            this.schemaString = schemaString;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
            this.datumReader = new GenericDatumReader<>(schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            final byte[] bytes = c.element().getPayload();
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
            final GenericRecord record = new GenericData.Record(schema);
            try {
                c.output(datumReader.read(record, decoder));
            } catch (Exception e) {
                LOG.error("Failed to parse avro record: " + c.element() + ", cause: " + e.getMessage());
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                } else {
                    throw e;
                }
            }
        }

    }

    private static class JsonToRowDoFn extends DoFn<PubsubMessage, Row> {

        private final TupleTag<PubsubMessage> failuresTag;

        private final boolean sendDeadletter;
        private final org.apache.beam.sdk.schemas.Schema schema;
        private final boolean validateUnnecessaryJsonField;

        JsonToRowDoFn(final boolean sendDeadletter,
                      final org.apache.beam.sdk.schemas.Schema schema,
                      final TupleTag<PubsubMessage> failuresTag,
                      final boolean validateUnnecessaryJsonField) {

            this.sendDeadletter = sendDeadletter;
            this.schema = schema;
            this.failuresTag = failuresTag;
            this.validateUnnecessaryJsonField = validateUnnecessaryJsonField;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            try {
                if(validateUnnecessaryJsonField && !JsonToRowConverter.validateSchema(schema, json)) {
                    throw new IllegalStateException("Validation error for json: " + json + " with schema: " + schema);
                }
                final Row row = JsonToRowConverter.convert(schema, json);
                c.output(row);
            } catch (Exception e) {
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                    LOG.error("Failed to parse json: " + json + ", cause: " + e.getMessage());
                } else {
                    throw e;
                }
            }
        }

    }

    private static class JsonToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private final TupleTag<PubsubMessage> failuresTag;

        private final boolean sendDeadletter;
        private final String schemaString;
        private transient Schema schema;
        private final boolean validateUnnecessaryJsonField;

        JsonToRecordDoFn(final boolean sendDeadletter,
                         final String schemaString,
                         final TupleTag<PubsubMessage> failuresTag,
                         final boolean validateUnnecessaryJsonField) {

            this.sendDeadletter = sendDeadletter;
            this.schemaString = schemaString;
            this.failuresTag = failuresTag;
            this.validateUnnecessaryJsonField = validateUnnecessaryJsonField;
        }

        @Setup
        public void setup() {
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            try {
                if(validateUnnecessaryJsonField && !JsonToRecordConverter.validateSchema(schema, json)) {
                    throw new IllegalStateException("Validation error for json: " + json + " with schema: " + schema);
                }
                final GenericRecord record = JsonToRecordConverter.convert(schema, json);
                c.output(record);
            } catch (Exception e) {
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                    LOG.error("Failed to parse json: " + json + ", cause: " + e.getMessage());
                } else {
                    throw e;
                }
            }
        }

    }

    private static class ProtoToRowDoFn extends DoFn<PubsubMessage, Row> {

        private final TupleTag<PubsubMessage> failuresTag;
        private final boolean sendDeadletter;

        private final String messageName;
        private final String descriptorPath;

        private transient org.apache.beam.sdk.schemas.Schema schema;
        private transient Descriptors.Descriptor descriptor;
        private transient JsonFormat.Printer printer;

        ProtoToRowDoFn(final boolean sendDeadletter,
                       final String messageName,
                       final String descriptorPath,
                       final TupleTag<PubsubMessage> failuresTag) {

            this.sendDeadletter = sendDeadletter;
            this.messageName = messageName;
            this.descriptorPath = descriptorPath;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            final byte[] bytes = StorageUtil.readBytes(descriptorPath);
            final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
            this.descriptor = descriptors.get(messageName);
            this.schema = ProtoToRowConverter.convertSchema(descriptor);

            final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
            descriptors.forEach((k, v) -> builder.add(v));
            this.printer = JsonFormat.printer().usingTypeRegistry(builder.build());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            try {
                final Row row = ProtoToRowConverter.convert(schema, descriptor, content, printer);
                c.output(row);
            } catch (Exception e) {
                LOG.error("Failed to deserialize protobuf. messageId: " + c.element().getMessageId());
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                } else {
                    throw e;
                }
            }
        }

    }

    private static class ProtoToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private final TupleTag<PubsubMessage> failuresTag;
        private final boolean sendDeadletter;

        private final String messageName;
        private final String descriptorPath;

        private transient Schema schema;
        private transient Descriptors.Descriptor descriptor;
        private transient JsonFormat.Printer printer;

        ProtoToRecordDoFn(final boolean sendDeadletter,
                          final String messageName,
                          final String descriptorPath,
                          final TupleTag<PubsubMessage> failuresTag) {

            this.sendDeadletter = sendDeadletter;
            this.messageName = messageName;
            this.descriptorPath = descriptorPath;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            final byte[] bytes = StorageUtil.readBytes(descriptorPath);
            final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
            this.descriptor = descriptors.get(messageName);
            this.schema = ProtoToRecordConverter.convertSchema(descriptor);

            final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
            descriptors.forEach((k, v) -> builder.add(v));
            this.printer = JsonFormat.printer().usingTypeRegistry(builder.build());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            try {
                final GenericRecord row = ProtoToRecordConverter.convert(schema, descriptor, content, printer);
                c.output(row);
            } catch (Exception e) {
                LOG.error("Failed to deserialize protobuf. messageId: " + c.element().getMessageId());
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                } else {
                    throw e;
                }
            }
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
            c.output(PubSubToRecordConverter.convertMessage(schema, c.element(), c.timestamp()));
        }

    }

}
