package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class PubSubSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

    private class PubSubSinkParameters implements Serializable {

        private String topic;
        private Format format;
        private List<String> attributes;
        private String idAttribute;
        private String timestampAttribute;

        private String protobufDescriptorPath;
        private String protobufMessageName;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<String> attributes) {
            this.attributes = attributes;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public void setIdAttribute(String idAttribute) {
            this.idAttribute = idAttribute;
        }

        public String getTimestampAttribute() {
            return timestampAttribute;
        }

        public void setTimestampAttribute(String timestampAttribute) {
            this.timestampAttribute = timestampAttribute;
        }

        public String getProtobufDescriptorPath() {
            return protobufDescriptorPath;
        }

        public void setProtobufDescriptorPath(String protobufDescriptorPath) {
            this.protobufDescriptorPath = protobufDescriptorPath;
        }

        public String getProtobufMessageName() {
            return protobufMessageName;
        }

        public void setProtobufMessageName(String protobufMessageName) {
            this.protobufMessageName = protobufMessageName;
        }

        public Integer getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(Integer maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public Integer getMaxBatchBytesSize() {
            return maxBatchBytesSize;
        }

        public void setMaxBatchBytesSize(Integer maxBatchBytesSize) {
            this.maxBatchBytesSize = maxBatchBytesSize;
        }
    }

    public String getName() { return "pubsub"; }

    private enum Format {
        avro,
        json,
        protobuf
    }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        write(input, config, waits);
        return new HashMap<>();
    }

    private static void write(final FCollection<?> collection, final SinkConfig config) {
        write(collection, config, null);
    }

    private static void write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final PubSubSinkParameters parameters = new Gson().fromJson(config.getParameters(), PubSubSinkParameters.class);
        final Write write = new Write(collection, parameters, waits);
        final PDone output = collection.getCollection().apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
    }

    private static class Write extends PTransform<PCollection<?>, PDone> {

        private static final Logger LOG = LoggerFactory.getLogger(Write.class);

        private FCollection<?> collection;
        private final PubSubSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private Write(final FCollection<?> collection,
                      final PubSubSinkParameters parameters,
                      final List<FCollection<?>> waits) {

            this.collection = collection;
            this.parameters = parameters;
            this.waits = waits;
        }

        public PDone expand(final PCollection<?> input) {

            validateParameters();
            setDefaultParameters();

            PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(parameters.getTopic());
            if(parameters.getIdAttribute() != null) {
                write = write.withIdAttribute(parameters.getIdAttribute());
            }
            if(parameters.getTimestampAttribute() != null) {
                write = write.withTimestampAttribute(parameters.getTimestampAttribute());
            }
            if(parameters.getMaxBatchSize() != null) {
                write = write.withMaxBatchSize(parameters.getMaxBatchSize());
            }
            if(parameters.getMaxBatchBytesSize() != null) {
                write = write.withMaxBatchBytesSize(parameters.getMaxBatchBytesSize());
            }

            switch (parameters.getFormat()) {
                case avro: {
                    final PCollection<GenericRecord> records = input
                            .apply("ToRecord", DataTypeTransform.transform(collection, DataType.AVRO));
                    return records.apply("ToMessage", ParDo.of(new GenericRecordPubsubMessageDoFn(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    collection.getAvroSchema().toString())))
                            .apply("PublishPubSub", write);
                }
                case json: {
                    switch (collection.getDataType()) {
                        case ROW: {
                            final PCollection<Row> rows = (PCollection<Row>)input;
                            return rows.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    RowSchemaUtil::getAsString,
                                    RowToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case AVRO: {
                            final PCollection<GenericRecord> records = (PCollection<GenericRecord>)input;
                            return records.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    AvroSchemaUtil::getAsString,
                                    RecordToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case STRUCT: {
                            final PCollection<Struct> structs = (PCollection<Struct>)input;
                            return structs.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    StructSchemaUtil::getAsString,
                                    StructToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case ENTITY: {
                            final PCollection<Entity> entities = (PCollection<Entity>)input;
                            return entities.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    EntitySchemaUtil::getAsString,
                                    EntityToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        default:
                            throw new IllegalArgumentException("PubSubSink module not support data type: " + collection.getDataType());
                    }
                }
                case protobuf: {
                    switch (collection.getDataType()) {
                        case ROW: {
                            final PCollection<Row> rows = (PCollection<Row>)input;
                            return rows.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    parameters.getProtobufDescriptorPath(),
                                    parameters.getProtobufMessageName(),
                                    RowSchemaUtil::getAsString,
                                    RowToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case AVRO: {
                            final PCollection<GenericRecord> records = (PCollection<GenericRecord>)input;
                            return records.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    parameters.getProtobufDescriptorPath(),
                                    parameters.getProtobufMessageName(),
                                    AvroSchemaUtil::getAsString,
                                    RecordToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case STRUCT: {
                            final PCollection<Struct> structs = (PCollection<Struct>)input;
                            return structs.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    parameters.getProtobufDescriptorPath(),
                                    parameters.getProtobufMessageName(),
                                    StructSchemaUtil::getAsString,
                                    StructToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case ENTITY: {
                            final PCollection<Entity> entities = (PCollection<Entity>)input;
                            return entities.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    parameters.getProtobufDescriptorPath(),
                                    parameters.getProtobufMessageName(),
                                    EntitySchemaUtil::getAsString,
                                    EntityToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        default:
                            throw new IllegalArgumentException("PubSubSink module not support data type: " + collection.getDataType());
                    }
                }
                default:
                    throw new IllegalArgumentException("PubSubSink module not support format: " + parameters.getFormat());
            }
        }

        private void validateParameters() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("PubSub parameters must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getTopic() == null) {
                errorMessages.add("PubSub sink module parameter must contain topic");
            }
            if(parameters.getFormat() == null) {
                errorMessages.add("PubSub sink module parameter must contain format");
            }
            if(parameters.getFormat() != null) {
                if(parameters.getFormat().equals(Format.protobuf)) {
                    if(parameters.getProtobufDescriptorPath() == null) {
                        errorMessages.add("PubSub sink module parameter must contain protobufDescriptorPath when set format `protobuf`");
                    }
                    if(parameters.getProtobufMessageName() == null) {
                        errorMessages.add("PubSub sink module parameter must contain protobufMessageName when set format `protobuf`");
                    }
                }
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if(parameters.getAttributes() == null) {
                parameters.setAttributes(new ArrayList<>());
            }
            if(parameters.getIdAttribute() != null
                    && !parameters.getAttributes().contains(parameters.getIdAttribute())) {
                parameters.getAttributes().add(parameters.getIdAttribute());
            }
        }


        private static class JsonPubsubMessageDoFn<T> extends PubsubMessageDoFn<T> {

            private final FieldGetter<T> getter;
            private final JsonConverter<T> converter;

            JsonPubsubMessageDoFn(final List<String> attributes,
                                  final String idAttribute,
                                  final FieldGetter<T> getter,
                                  final JsonConverter<T> converter) {

                super(attributes, idAttribute);
                this.getter = getter;
                this.converter = converter;
            }

            @Override
            String getString(T element, String fieldName) {
                return getter.getString(element, fieldName);
            }

            @Override
            byte[] encode(T element) {
                final String json = converter.convert(element);
                if(json == null) {
                    return null;
                }
                return json.getBytes(StandardCharsets.UTF_8);
            }

        }

        private static class GenericRecordPubsubMessageDoFn extends PubsubMessageDoFn<GenericRecord> {

            private final String schemaString;

            private transient Schema schema;
            private transient DatumWriter<GenericRecord> writer;
            private transient BinaryEncoder encoder;

            GenericRecordPubsubMessageDoFn(final List<String> attributes,
                                           final String idAttribute,
                                           final String schemaString) {

                super(attributes, idAttribute);
                this.schemaString = schemaString;
            }

            @Setup
            public void setup() {
                this.schema = new Schema.Parser().parse(schemaString);
                this.writer = new GenericDatumWriter<>(schema);
                this.encoder = null;
            }

            @Override
            String getString(GenericRecord record, String fieldName) {
                return AvroSchemaUtil.getAsString(record, fieldName);
            }

            @Override
            byte[] encode(GenericRecord record) {
                try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                    encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
                    writer.write(record, encoder);
                    encoder.flush();
                    return byteArrayOutputStream.toByteArray();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to encode record: " + record.toString(), e);
                }
            }

        }

        private static class ProtobufPubsubMessageDoFn<T> extends PubsubMessageDoFn<T> {

            private final String descriptorPath;
            private final String messageName;
            private final FieldGetter<T> getter;
            private final ProtoConverter<T> converter;

            private transient Descriptors.Descriptor descriptor;

            ProtobufPubsubMessageDoFn(final List<String> attributes,
                                      final String idAttribute,
                                      final String descriptorPath,
                                      final String messageName,
                                      final FieldGetter<T> getter,
                                      final ProtoConverter<T> converter) {

                super(attributes, idAttribute);
                this.descriptorPath = descriptorPath;
                this.messageName = messageName;
                this.getter = getter;
                this.converter = converter;
            }

            @Setup
            public void setup() {
                final byte[] bytes = StorageUtil.readBytes(descriptorPath);
                final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
                this.descriptor = descriptors.get(messageName);
            }

            @Override
            String getString(T element, String fieldName) {
                return getter.getString(element, fieldName);
            }

            @Override
            byte[] encode(T element) {
                final DynamicMessage message = converter.convert(descriptor, element);
                if(message == null) {
                    return null;
                }
                return message.toByteArray();
            }

        }

        private static abstract class PubsubMessageDoFn<T> extends DoFn<T, PubsubMessage> {

            private final List<String> attributes;
            private final String idAttribute;

            PubsubMessageDoFn(final List<String> attributes, final String idAttribute) {
                this.attributes = attributes;
                this.idAttribute = idAttribute;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final Map<String, String> attributeMap = getAttributes(element);
                final String attributeId = getIdAttribute(element);
                final byte[] payload = encode(element);
                final PubsubMessage message = new PubsubMessage(payload, attributeMap, attributeId);
                c.output(message);
            }

            private Map<String, String> getAttributes(T element) {
                final Map<String, String> attributeMap = new HashMap<>();
                if(attributes == null || attributes.size() == 0) {
                    return attributeMap;
                }
                for(final String attribute : attributes) {
                    final String value = getString(element, attribute);
                    if(value == null) {
                        continue;
                    }
                    attributeMap.put(attribute, value);
                }
                return attributeMap;
            }

            private String getIdAttribute(T element) {
                if(idAttribute == null) {
                    return null;
                }
                return getString(element, idAttribute);
            }

            abstract String getString(T element, String fieldName);

            abstract byte[] encode(T element);

        }
    }

    private interface FieldGetter<T> extends Serializable {
        String getString(final T value, final String field);
    }

    private interface JsonConverter<T> extends Serializable {
        String convert(final T value);
    }

    private interface ProtoConverter<T> extends Serializable {
        DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final T value);
    }

}