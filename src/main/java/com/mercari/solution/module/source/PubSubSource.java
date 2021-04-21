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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
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
import java.util.stream.Collectors;


public class PubSubSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

    private class PubSubSourceParameters implements Serializable {

        private String topic;
        private String subscription;

        private Format format;
        private String idAttribute;
        private OutputType outputType;

        private String messageName;

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

        public OutputType getOutputType() {
            return outputType;
        }

        public void setOutputType(OutputType outputType) {
            this.outputType = outputType;
        }

        public String getMessageName() {
            return messageName;
        }

        public void setMessageName(String messageName) {
            this.messageName = messageName;
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
                        throw new IllegalStateException("PubSub source module does not support outputType: " + parameters.getOutputType());
                }
            }
            case protobuf: {
                final byte[] descriptorBytes = StorageUtil.readBytes(config.getSchema().getProtobufDescriptor());
                final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(descriptorBytes);
                final Descriptors.Descriptor messageDescriptor = descriptors.get(parameters.getMessageName());
                switch (parameters.getOutputType()) {
                    case avro: {
                        final Schema avroSchema = ProtoToRecordConverter.convertSchema(messageDescriptor);
                        final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                    }
                    case row: {
                        final org.apache.beam.sdk.schemas.Schema rowSchema = ProtoToRowConverter.convertSchema(messageDescriptor);
                        final PubSubStream<Row> stream = new PubSubStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        return FCollection.of(config.getName(), output, DataType.ROW, rowSchema);
                    }
                    default:
                        throw new IllegalStateException("PubSub source module does not support outputType: " + parameters.getOutputType());
                }
            }
            case message: {
                final Schema avroSchema = PubSubToRecordConverter.createMessageSchema();
                final PubSubStream<GenericRecord> stream = new PubSubStream<>(config, parameters);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
            }
            default:
                throw new IllegalStateException("PubSub source module does not support format: " + parameters.getFormat());
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
                case protobuf:
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

        private final SourceConfig.InputSchema schema;
        private final String timestampAttribute;
        private final PubSubSourceParameters parameters;

        private PubSubStream(final SourceConfig config, final PubSubSourceParameters parameters) {
            this.schema = config.getSchema();
            this.timestampAttribute = config.getTimestampAttribute();
            this.parameters = parameters;
        }

        public PCollection<T> expand(final PBegin begin) {

            PubsubIO.Read read;
            switch (parameters.getFormat()) {
                case avro: {
                    read = PubsubIO.readAvroGenericRecords(SourceConfig.convertAvroSchema(schema));
                    break;
                }
                case protobuf:
                case json:
                case message: {
                    read = PubsubIO.readMessagesWithAttributesAndMessageId();
                    break;
                }
                default:
                    throw new IllegalArgumentException("PubSubSource must be set format avro or message. " + parameters.getFormat());
            }

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
                deadletterTopic = getDeadLetterTopic(parameters.getSubscription());
                sendDeadletter = deadletterTopic != null;
            }

            final TupleTag<PubsubMessage> failuresTag = new TupleTag<>() {
            };

            // Deserialize pubsub messages
            final PCollection<T> messages;
            final PCollection<PubsubMessage> failures;
            switch (parameters.getFormat()) {
                case avro: {
                    messages = (PCollection<T>) begin.apply("ReadPubSubRecord", (PubsubIO.Read<GenericRecord>) read);
                    failures = null;
                    break;
                }
                case json: {
                    final PCollection<PubsubMessage> pubsubMessages = begin
                            .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read);
                    switch (parameters.getOutputType()) {
                        case avro: {
                            final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {
                            };
                            final Schema avroSchema = SourceConfig.convertAvroSchema(schema);
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("JsonToRecord", ParDo
                                            .of(new JsonToRecordDoFn(sendDeadletter, avroSchema.toString(), failuresTag))
                                            .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputAvroTag)
                                    .setCoder(AvroCoder.of(avroSchema));
                            failures = tuple.get(failuresTag);
                            break;
                        }
                        case row: {
                            final TupleTag<Row> outputRowTag = new TupleTag<>() {
                            };
                            final org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(schema);
                            final PCollectionTuple tuple = pubsubMessages
                                    .apply("JsonToRow", ParDo
                                            .of(new JsonToRowDoFn(sendDeadletter, rowSchema, failuresTag))
                                            .withOutputTags(outputRowTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputRowTag)
                                    .setCoder(RowCoder.of(rowSchema));
                            failures = tuple.get(failuresTag);
                            break;
                        }
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                }
                case protobuf: {
                    final PCollection<PubsubMessage> pubsubMessages = begin
                            .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read);
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
                    break;
                }
                case message: {
                    messages = (PCollection<T>) begin
                            .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>) read)
                            .apply("MessageToRecord", ParDo.of(new MessageToRecordDoFn()))
                            .setCoder(AvroCoder.of(PubSubToRecordConverter.createMessageSchema()));
                    failures = null;
                    break;
                }
                default:
                    throw new IllegalArgumentException();
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
                }
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static class JsonToRowDoFn extends DoFn<PubsubMessage, Row> {

        private final TupleTag<PubsubMessage> failuresTag;

        private final boolean sendDeadletter;
        private final org.apache.beam.sdk.schemas.Schema schema;

        JsonToRowDoFn(final boolean sendDeadletter,
                      final org.apache.beam.sdk.schemas.Schema schema,
                      final TupleTag<PubsubMessage> failuresTag) {

            this.sendDeadletter = sendDeadletter;
            this.schema = schema;
            this.failuresTag = failuresTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final byte[] content = c.element().getPayload();
            final String json = new String(content, StandardCharsets.UTF_8);
            try {
                final Row row = JsonToRowConverter.convert(schema, json);
                c.output(row);
            } catch (Exception e) {
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                    LOG.error("Failed to parse json: " + json);
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

        JsonToRecordDoFn(final boolean sendDeadletter,
                         final String schemaString,
                         final TupleTag<PubsubMessage> failuresTag) {

            this.sendDeadletter = sendDeadletter;
            this.schemaString = schemaString;
            this.failuresTag = failuresTag;
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
                final GenericRecord record = JsonToRecordConverter.convert(schema, json);
                c.output(record);
            } catch (Exception e) {
                if(sendDeadletter) {
                    c.output(failuresTag, c.element());
                    LOG.error("Failed to parse json: " + json);
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
            c.output(PubSubToRecordConverter.convertMessage(schema, c.element()));
        }

    }

}
