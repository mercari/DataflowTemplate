package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
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

        private String messageTopicField;
        private String messageIdField;
        private String messageTimestampField;
        private String messageOrderingKeyField;
        private Map<String, String> messageAttributesFields;

        private Boolean validateUnnecessaryJsonField;

        private Boolean failFast;

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

        public String getMessageTopicField() {
            return messageTopicField;
        }

        public String getMessageIdField() {
            return messageIdField;
        }

        public String getMessageTimestampField() {
            return messageTimestampField;
        }

        public String getMessageOrderingKeyField() {
            return messageOrderingKeyField;
        }

        public Map<String, String> getMessageAttributesFields() {
            return messageAttributesFields;
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
                errorMessages.add("pubsub source module[" + name + "].parameters requires topic or subscription");
            }
            if(format == null) {
                errorMessages.add("pubsub source module[" + name + "].parameters requires format");
            } else {
                switch (format) {
                    case protobuf -> {
                        if(this.messageName == null) {
                            errorMessages.add("pubsub source module[" + name + "].parameters requires messageName if format is parquet");
                        }
                    }
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(messageAttributesFields == null) {
                messageAttributesFields = new HashMap<>();
            }
            if(outputType == null) {
                outputType = switch (format) {
                    case avro, message -> OutputType.avro;
                    case protobuf, json -> OutputType.row;
                    default -> outputType = OutputType.row;
                };
            }
            if(validateUnnecessaryJsonField == null) {
                validateUnnecessaryJsonField = false;
            }
        }

        public static PubSubSourceParameters of(final SourceConfig config, final PBegin begin) {
            final PubSubSourceParameters parameters = new Gson().fromJson(config.getParameters(), PubSubSourceParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("pubsub source module[" + config.getName() + "] parameters must not be empty!");
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
        return PubSubSource.stream(begin, config);
    }

    public static Map<String, FCollection<?>> stream(final PBegin begin, final SourceConfig config) {

        final String outputFailuresName = String.format("%s.failures", config.getName());
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        final PubSubSourceParameters parameters = PubSubSourceParameters.of(config, begin);
        switch (parameters.getFormat()) {
            case avro -> {
                final Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                final FCollection<?> fCollection = FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                outputs.put(config.getName(), fCollection);
            }
            case json -> {
                switch (parameters.getOutputType()) {
                    case avro -> {
                        final Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                        final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        final FCollection<?> fCollection = FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                        outputs.put(config.getName(), fCollection);
                    }
                    case row -> {
                        final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(config.getSchema());
                        final PubSubStream<Row> stream = new PubSubStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        final FCollection<?> fCollection = FCollection.of(config.getName(), output, DataType.ROW, rowSchema);
                        outputs.put(config.getName(), fCollection);
                    }
                    default -> throw new IllegalStateException("PubSub source module does not support outputType: " + parameters.getOutputType());
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
                switch (parameters.getOutputType()) {
                    case avro -> {
                        final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        final AvroCoder<GenericRecord> avroCoder = (AvroCoder<GenericRecord>) output.getCoder();
                        final FCollection<?> fCollection = FCollection.of(config.getName(), output, DataType.AVRO, avroCoder.getSchema());
                        outputs.put(config.getName(), fCollection);
                    }
                    case row -> {
                        final PubSubStream<Row> stream = new PubSubStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        final RowCoder rowCoder = (RowCoder) output.getCoder();
                        final FCollection<?> fCollection = FCollection.of(config.getName(), output, DataType.ROW, rowCoder.getSchema());
                        outputs.put(config.getName(), fCollection);
                    }
                    default -> throw new IllegalStateException("PubSub source module does not support outputType: " + parameters.getOutputType());
                }
            }
            case message -> {
                final Schema avroSchema = PubSubToRecordConverter.createMessageSchema();
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                final FCollection<?> fCollection = FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                outputs.put(config.getName(), fCollection);
            }
            default -> throw new IllegalStateException("PubSub source module does not support format: " + parameters.getFormat());
        }

        return outputs;
    }

    private static Schema createSchema(
            final SchemaBuilder.FieldAssembler<Schema> schemaBuilder,
            final PubSubSourceParameters parameters) {

        if(parameters.getMessageTopicField() != null) {
            schemaBuilder.name(parameters.getMessageTopicField()).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
        }
        if(parameters.getMessageIdField() != null) {
            schemaBuilder.name(parameters.getMessageIdField()).type(AvroSchemaUtil.REQUIRED_STRING).noDefault();
        }
        if(parameters.getMessageTimestampField() != null) {
            schemaBuilder.name(parameters.getMessageTimestampField()).type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault();
        }
        if(parameters.getMessageOrderingKeyField() != null) {
            schemaBuilder.name(parameters.getMessageOrderingKeyField()).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
        }
        if(!parameters.getMessageAttributesFields().isEmpty()) {
            for(final Map.Entry<String, String> entry : parameters.getMessageAttributesFields().entrySet()) {
                schemaBuilder.name(entry.getValue()).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
            }
        }
        return schemaBuilder.endRecord();
    }

    private static org.apache.beam.sdk.schemas.Schema createSchema(
            final org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder,
            final PubSubSourceParameters parameters) {

        if(parameters.getMessageTopicField() != null) {
            schemaBuilder.addField(parameters.getMessageTopicField(), org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true));
        }
        if(parameters.getMessageIdField() != null) {
            schemaBuilder.addField(parameters.getMessageIdField(), org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true));
        }
        if(parameters.getMessageTimestampField() != null) {
            schemaBuilder.addField(parameters.getMessageTimestampField(), org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true));
        }
        if(parameters.getMessageOrderingKeyField() != null) {
            schemaBuilder.addField(parameters.getMessageOrderingKeyField(), org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true));
        }
        if(!parameters.getMessageAttributesFields().isEmpty()) {
            for(final Map.Entry<String, String> entry : parameters.getMessageAttributesFields().entrySet()) {
                schemaBuilder.addField(entry.getValue(), org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true));
            }
        }
        return schemaBuilder.build();
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

            PubsubIO.Read read = PubsubIO.readMessagesWithAttributesAndMessageIdAndOrderingKey();
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

            final TupleTag<PubsubMessage> failuresTag = new TupleTag<>() {};

            // Deserialize pubsub messages
            final PCollection<PubsubMessage> pubsubMessages = begin
                    .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read);

            final PCollection<T> messages;
            final PCollection<PubsubMessage> failures;
            switch (parameters.getFormat()) {
                case avro -> {
                    final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {};
                    final Schema avroSchema = SourceConfig.convertAvroSchema(schema);
                    final PCollectionTuple tuple = pubsubMessages
                            .apply("AvroToRecord", ParDo
                                    .of(new AvroToRecordDoFn(false, avroSchema.toString(), failuresTag))
                                    .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                    messages = (PCollection<T>) tuple.get(outputAvroTag).setCoder(AvroCoder.of(avroSchema));
                    failures = tuple.get(failuresTag);
                }
                case json -> {
                    switch (parameters.getOutputType()) {
                        case avro -> {
                            final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {};
                            final Schema avroSchema = SourceConfig.convertAvroSchema(schema);
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("JsonToRecord", ParDo
                                            .of(new JsonToRecordDoFn(false, avroSchema.toString(), failuresTag, parameters.getValidateUnnecessaryJsonField()))
                                            .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputAvroTag)
                                    .setCoder(AvroCoder.of(avroSchema));
                            failures = tuple.get(failuresTag);
                        }
                        case row -> {
                            final TupleTag<Row> outputRowTag = new TupleTag<>() {};
                            final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(schema);
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("JsonToRow", ParDo
                                            .of(new JsonToRowDoFn(false, rowSchema, failuresTag, parameters.getValidateUnnecessaryJsonField()))
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
                    final Descriptors.Descriptor descriptor = descriptors.get(parameters.getMessageName());
                    switch (parameters.getOutputType()) {
                        case avro -> {
                            final SchemaBuilder.FieldAssembler<Schema> avroSchemaBuilder = ProtoToRecordConverter.convertSchemaBuilder(descriptor);
                            final Schema avroSchema = createSchema(avroSchemaBuilder, parameters);
                            final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {};
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("ProtobufToRecord", ParDo
                                            .of(new ProtoToRecordDoFn(
                                                    parameters,
                                                    schema.getProtobufDescriptor(),
                                                    avroSchema.toString(),
                                                    false,
                                                    failuresTag))
                                            .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputAvroTag)
                                    .setCoder(AvroCoder.of(avroSchema));
                            failures = tuple.get(failuresTag);
                        }
                        case row -> {
                            final org.apache.beam.sdk.schemas.Schema.Builder rowSchemaBuilder = ProtoToRowConverter.convertSchemaBuilder(descriptor);
                            final org.apache.beam.sdk.schemas.Schema rowSchema = createSchema(rowSchemaBuilder, parameters);
                            final TupleTag<Row> outputRowTag = new TupleTag<>() {};
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("ProtobufToRow", ParDo
                                            .of(new ProtoToRowDoFn(
                                                    parameters,
                                                    schema.getProtobufDescriptor(),
                                                    rowSchema,
                                                    false,
                                                    failuresTag))
                                            .withOutputTags(outputRowTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputRowTag)
                                    .setCoder(RowCoder.of(rowSchema));
                            failures = tuple.get(failuresTag);
                        }
                        default -> throw new IllegalStateException();
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

            //if (failures != null && deadletterTopic != null) {
            //    failures.apply("PublishDeadLetter", PubsubIO.writeMessages().to(deadletterTopic));
            //}

            return messages;
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

        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();// Collections.synchronizedMap(new HashMap<>());
        private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();

        private final TupleTag<PubsubMessage> failuresTag;
        private final boolean sendDeadletter;

        private final String messageName;
        private final String descriptorPath;
        private final org.apache.beam.sdk.schemas.Schema schema;

        private final String messageTopicField;
        private final String messageIdField;
        private final String messageTimestampField;
        private final String messageOrderingKeyField;
        private final Map<String, String> messageAttributesFields;

        ProtoToRowDoFn(final PubSubSourceParameters parameters,
                       final String descriptorPath,
                       final org.apache.beam.sdk.schemas.Schema schema,
                       final boolean sendDeadletter,
                       final TupleTag<PubsubMessage> failuresTag) {

            this.schema = schema;
            this.messageName = parameters.getMessageName();
            this.descriptorPath = descriptorPath;

            this.messageTopicField = parameters.getMessageTopicField();
            this.messageIdField = parameters.getMessageIdField();
            this.messageTimestampField = parameters.getMessageTimestampField();
            this.messageOrderingKeyField = parameters.getMessageOrderingKeyField();
            this.messageAttributesFields = parameters.getMessageAttributesFields();

            this.sendDeadletter = sendDeadletter;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            LOG.info("Start setup ProtoToRow DoFn thread id: {}", Thread.currentThread().getId());
            long start = Instant.now().toEpochMilli();
            final Descriptors.Descriptor descriptor = getOrLoadDescriptor(descriptors, printers, messageName, descriptorPath);
            long end = Instant.now().toEpochMilli();
            LOG.info("Finished setup ProtoToRow DoFn {} ms, thread id: {}, with descriptor: {}", (end - start), Thread.currentThread().getId(), descriptor.getFullName());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final PubsubMessage message = c.element();
            if(message == null) {
                LOG.warn("pubsub message is null");
                return;
            }
            final byte[] content = message.getPayload();
            try {
                final Descriptors.Descriptor descriptor = Optional
                        .ofNullable(descriptors.get(messageName))
                        .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, descriptorPath));
                final JsonFormat.Printer printer = printers.get(messageName);

                final Row.FieldValueBuilder builder = ProtoToRowConverter.convertBuilder(schema, descriptor, content, printer);
                if(messageTopicField != null) {
                    builder.withFieldValue(messageTopicField, message.getTopic());
                }
                if(messageIdField != null) {
                    builder.withFieldValue(messageIdField, message.getMessageId());
                }
                if(messageTimestampField != null) {
                    builder.withFieldValue(messageTimestampField, c.timestamp());
                }
                if(messageOrderingKeyField != null) {
                    builder.withFieldValue(messageOrderingKeyField, message.getOrderingKey());
                }
                if(!messageAttributesFields.isEmpty()) {
                    for(final Map.Entry<String, String> entry : messageAttributesFields.entrySet()) {
                        builder.withFieldValue(entry.getValue(), message.getAttribute(entry.getKey()));
                    }
                }
                c.output(builder.build());
            } catch (Exception e) {
                LOG.error("Failed to deserialize protobuf. messageId: " + message.getMessageId());
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                } else {
                    throw e;
                }
            }
        }

    }

    private static class ProtoToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private static final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
        private static final Map<String, JsonFormat.Printer> printers = new HashMap<>();

        private final TupleTag<PubsubMessage> failuresTag;
        private final boolean sendDeadletter;

        private final String messageName;
        private final String descriptorPath;
        private final String avroSchemaString;

        private final String messageTopicField;
        private final String messageIdField;
        private final String messageTimestampField;
        private final String messageOrderingKeyField;
        private final Map<String, String> messageAttributesFields;

        private transient Schema schema;


        ProtoToRecordDoFn(final PubSubSourceParameters parameters,
                          final String descriptorPath,
                          final String avroSchemaString,
                          final boolean sendDeadletter,
                          final TupleTag<PubsubMessage> failuresTag) {

            this.messageName = parameters.getMessageName();
            this.descriptorPath = descriptorPath;
            this.avroSchemaString = avroSchemaString;

            this.messageTopicField = parameters.getMessageTopicField();
            this.messageIdField = parameters.getMessageIdField();
            this.messageTimestampField = parameters.getMessageTimestampField();
            this.messageOrderingKeyField = parameters.getMessageOrderingKeyField();
            this.messageAttributesFields = parameters.getMessageAttributesFields();

            this.sendDeadletter = sendDeadletter;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            LOG.info("Start setup ProtoToRecord DoFn thread id: {}", Thread.currentThread().getId());
            this.schema = AvroSchemaUtil.convertSchema(avroSchemaString);
            long start = Instant.now().toEpochMilli();
            final Descriptors.Descriptor descriptor = getOrLoadDescriptor(descriptors, printers, messageName, descriptorPath);
            long end = Instant.now().toEpochMilli();
            LOG.info("Finished setup ProtoToRecord DoFn {} ms, thread id: {}, with descriptor: {}", (end - start), Thread.currentThread().getId(), descriptor.getFullName());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final PubsubMessage message = c.element();
            if(message == null) {
                return;
            }
            final byte[] content = message.getPayload();
            try {
                final Descriptors.Descriptor descriptor = Optional
                        .ofNullable(descriptors.get(messageName))
                        .orElseGet(() -> getOrLoadDescriptor(descriptors, printers, messageName, descriptorPath));
                final JsonFormat.Printer printer = printers.get(messageName);
                final GenericRecordBuilder recordBuilder = ProtoToRecordConverter.convertBuilder(schema, descriptor, content, printer);

                if(messageTopicField != null) {
                    recordBuilder.set(messageTopicField, message.getTopic());
                }
                if(messageIdField != null) {
                    recordBuilder.set(messageIdField, message.getMessageId());
                }
                if(messageTimestampField != null) {
                    recordBuilder.set(messageTimestampField, c.timestamp().getMillis() * 1000L);
                }
                if(messageOrderingKeyField != null) {
                    recordBuilder.set(messageOrderingKeyField, message.getOrderingKey());
                }
                if(!messageAttributesFields.isEmpty()) {
                    for(final Map.Entry<String, String> entry : messageAttributesFields.entrySet()) {
                        recordBuilder.set(entry.getValue(), message.getAttribute(entry.getKey()));
                    }
                }

                c.output(recordBuilder.build());
            } catch (Exception e) {
                LOG.error("Failed to deserialize protobuf. messageId: " + message.getMessageId());
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

    synchronized static private Descriptors.Descriptor getOrLoadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Map<String, JsonFormat.Printer> printers,
            final String messageName,
            final String path) {

        if(descriptors.containsKey(messageName)) {
            final Descriptors.Descriptor descriptor = descriptors.get(messageName);
            if(descriptor != null) {
                return descriptor;
            } else {
                descriptors.remove(messageName);
            }
        }
        loadDescriptor(descriptors, printers, messageName, path);
        return descriptors.get(messageName);
    }

    synchronized static private void loadDescriptor(
            final Map<String, Descriptors.Descriptor> descriptors,
            final Map<String, JsonFormat.Printer> printers,
            final String messageName,
            final String descriptorPath) {

        if(descriptors.containsKey(messageName) && descriptors.get(messageName) == null) {
            descriptors.remove(messageName);
        }

        if(!descriptors.containsKey(messageName)) {
            final byte[] bytes = StorageUtil.readBytes(descriptorPath);
            final Map<String, Descriptors.Descriptor> map = ProtoSchemaUtil.getDescriptors(bytes);
            if(!map.containsKey(messageName)) {
                throw new IllegalArgumentException();
            }

            descriptors.put(messageName, map.get(messageName));

            final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
            map.forEach((k, v) -> builder.add(v));
            final JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(builder.build());
            printers.put(messageName, printer);

            LOG.info("setup pubsub source module. protoMessage: {} loaded", messageName);
        } else {
            LOG.info("setup pubsub source module protoMessage: {} skipped loading", messageName);
        }
    }

}
