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
        private List<String> orderingKeyFields;

        private String protobufDescriptor;
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

        public List<String> getOrderingKeyFields() {
            return orderingKeyFields;
        }

        public void setOrderingKeyFields(List<String> orderingKeyFields) {
            this.orderingKeyFields = orderingKeyFields;
        }

        public String getProtobufDescriptor() {
            return protobufDescriptor;
        }

        public void setProtobufDescriptor(String protobufDescriptor) {
            this.protobufDescriptor = protobufDescriptor;
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

        private void validate() {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.getTopic() == null) {
                errorMessages.add("PubSub sink module parameter must contain topic");
            }
            if(this.getFormat() == null) {
                errorMessages.add("PubSub sink module parameter must contain format");
            }
            if(this.getFormat() != null) {
                if(this.getFormat().equals(Format.protobuf)) {
                    if(this.getProtobufDescriptor() == null) {
                        errorMessages.add("PubSub sink module parameter must contain protobufDescriptor when set format `protobuf`");
                    }
                    if(this.getProtobufMessageName() == null) {
                        errorMessages.add("PubSub sink module parameter must contain protobufMessageName when set format `protobuf`");
                    }
                }
            }
            if(this.maxBatchSize != null) {

            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaults() {
            if(this.getAttributes() == null) {
                this.setAttributes(new ArrayList<>());
            }
            if(this.idAttribute != null
                    && !this.attributes.contains(this.idAttribute)) {
                this.attributes.add(this.idAttribute);
            }
            if(this.orderingKeyFields == null) {
                this.orderingKeyFields = new ArrayList<>();
            } else if(this.orderingKeyFields.size() > 0 && (this.maxBatchSize == null || this.maxBatchSize != 1)) {
                LOG.warn("pubsub source module maxBatchSize must be 1 when using orderingKeyFields. ref: https://issues.apache.org/jira/browse/BEAM-13148");
                this.maxBatchSize = 1;
            }
        }
    }

    public String getName() { return "pubsub"; }

    private enum Format {
        avro,
        json,
        protobuf
    }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits, List<FCollection<?>> sideInputs) {
        write(input, config, waits, sideInputs);
        return new HashMap<>();
    }

    private static void write(final FCollection<?> collection, final SinkConfig config) {
        write(collection, config, null, null);
    }

    private static void write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits, final List<FCollection<?>> sideInputs) {
        final PubSubSinkParameters parameters = new Gson().fromJson(config.getParameters(), PubSubSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("PubSub source module parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

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

            PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(parameters.getTopic());
            if (parameters.getIdAttribute() != null) {
                write = write.withIdAttribute(parameters.getIdAttribute());
            }
            if (parameters.getTimestampAttribute() != null) {
                write = write.withTimestampAttribute(parameters.getTimestampAttribute());
            }
            if (parameters.getMaxBatchSize() != null) {
                write = write.withMaxBatchSize(parameters.getMaxBatchSize());
            }
            if (parameters.getMaxBatchBytesSize() != null) {
                write = write.withMaxBatchBytesSize(parameters.getMaxBatchBytesSize());
            }

            switch (parameters.getFormat()) {
                case avro: {
                    final PCollection<GenericRecord> records = input
                            .apply("ToRecord", DataTypeTransform.transform(collection, DataType.AVRO));
                    return records.apply("ToMessage", ParDo.of(new GenericRecordPubsubMessageDoFn(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    parameters.getOrderingKeyFields(),
                                    collection.getAvroSchema().toString())))
                            .apply("PublishPubSub", write);
                }
                case json: {
                    switch (collection.getDataType()) {
                        case ROW: {
                            final PCollection<Row> rows = (PCollection<Row>) input;
                            return rows.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            RowSchemaUtil::getAsString,
                                            RowToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case AVRO: {
                            final PCollection<GenericRecord> records = (PCollection<GenericRecord>) input;
                            return records.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            AvroSchemaUtil::getAsString,
                                            RecordToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case STRUCT: {
                            final PCollection<Struct> structs = (PCollection<Struct>) input;
                            return structs.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            StructSchemaUtil::getAsString,
                                            StructToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case ENTITY: {
                            final PCollection<Entity> entities = (PCollection<Entity>) input;
                            return entities.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
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
                            final PCollection<Row> rows = (PCollection<Row>) input;
                            return rows.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            RowSchemaUtil::getAsString,
                                            RowToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case AVRO: {
                            final PCollection<GenericRecord> records = (PCollection<GenericRecord>) input;
                            return records.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            AvroSchemaUtil::getAsString,
                                            RecordToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case STRUCT: {
                            final PCollection<Struct> structs = (PCollection<Struct>) input;
                            return structs.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            StructSchemaUtil::getAsString,
                                            StructToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case ENTITY: {
                            final PCollection<Entity> entities = (PCollection<Entity>) input;
                            return entities.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
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


        private static class JsonPubsubMessageDoFn<T> extends PubsubMessageDoFn<T> {

            private final SchemaUtil.StringGetter<T> stringGetter;
            private final JsonConverter<T> converter;

            JsonPubsubMessageDoFn(final List<String> attributes,
                                  final String idAttribute,
                                  final List<String> orderingKeyFields,
                                  final SchemaUtil.StringGetter<T> stringGetter,
                                  final JsonConverter<T> converter) {

                super(attributes, idAttribute, orderingKeyFields);
                this.stringGetter = stringGetter;
                this.converter = converter;
            }

            @Override
            String getString(T element, String fieldName) {
                return stringGetter.getAsString(element, fieldName);
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
                                           final List<String> orderingKeyFields,
                                           final String schemaString) {

                super(attributes, idAttribute, orderingKeyFields);
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
            private final SchemaUtil.StringGetter<T> stringGetter;
            private final ProtoConverter<T> converter;

            private transient Descriptors.Descriptor descriptor;

            ProtobufPubsubMessageDoFn(final List<String> attributes,
                                      final String idAttribute,
                                      final List<String> orderingKeyFields,
                                      final String descriptorPath,
                                      final String messageName,
                                      final SchemaUtil.StringGetter<T> stringGetter,
                                      final ProtoConverter<T> converter) {

                super(attributes, idAttribute, orderingKeyFields);
                this.descriptorPath = descriptorPath;
                this.messageName = messageName;
                this.stringGetter = stringGetter;
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
                return stringGetter.getAsString(element, fieldName);
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
            private final List<String> orderingKeyFields;

            PubsubMessageDoFn(final List<String> attributes, final String idAttribute, final List<String> orderingKeyFields) {
                this.attributes = attributes;
                this.idAttribute = idAttribute;
                this.orderingKeyFields = orderingKeyFields;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final Map<String, String> attributeMap = getAttributes(element);
                final String messageId = getMessageId(element);
                final String orderingKey = getOrderingKey(element);
                final byte[] payload = encode(element);
                final PubsubMessage message = new PubsubMessage(payload, attributeMap, messageId, orderingKey);
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

            private String getMessageId(T element) {
                if(idAttribute == null) {
                    return null;
                }
                return getString(element, idAttribute);
            }

            private String getOrderingKey(T element) {
                if(orderingKeyFields == null || orderingKeyFields.size() == 0) {
                    return null;
                }
                return getAttributesAsString(element, orderingKeyFields);
            }

            private String getAttributesAsString(final T element, final List<String> fields) {
                final StringBuilder sb = new StringBuilder();
                for(final String fieldName : fields) {
                    final String fieldValue = getString(element, fieldName);
                    sb.append(fieldValue == null ? "" : fieldValue);
                    sb.append("#");
                }
                if(sb.length() > 0) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                return sb.toString();
            }

            abstract String getString(T element, String fieldName);

            abstract byte[] encode(T element);

        }
    }

    private interface JsonConverter<T> extends Serializable {
        String convert(final T value);
    }

    private interface ProtoConverter<T> extends Serializable {
        DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final T value);
    }

}