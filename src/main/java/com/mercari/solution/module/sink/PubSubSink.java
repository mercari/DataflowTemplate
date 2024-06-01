package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import freemarker.template.Template;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
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
    private static final String ATTRIBUTE_NAME_TOPIC = "__topic__";

    private static class PubSubSinkParameters implements Serializable {

        private String topic;
        private Format format;
        private List<String> attributes;
        private String idAttribute;
        private String timestampAttribute;
        private List<String> idAttributeFields;
        private List<String> orderingKeyFields;

        private String protobufDescriptor;
        private String protobufMessageName;

        private Integer maxBatchSize;
        private Integer maxBatchBytesSize;


        public String getTopic() {
            return topic;
        }

        public Format getFormat() {
            return format;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public String getTimestampAttribute() {
            return timestampAttribute;
        }

        public List<String> getIdAttributeFields() {
            return idAttributeFields;
        }

        public List<String> getOrderingKeyFields() {
            return orderingKeyFields;
        }

        public String getProtobufDescriptor() {
            return protobufDescriptor;
        }

        public String getProtobufMessageName() {
            return protobufMessageName;
        }

        public Integer getMaxBatchSize() {
            return maxBatchSize;
        }

        public Integer getMaxBatchBytesSize() {
            return maxBatchBytesSize;
        }


        private void validate(final String name) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.topic == null) {
                errorMessages.add("pubsub sink module " + name + " parameter must contain topic or topicTemplate");
            }
            if(this.format == null) {
                errorMessages.add("pubsub sink module " + name + " parameter must contain format");
            } else {
                if(this.getFormat().equals(Format.protobuf)) {
                    if(this.getProtobufDescriptor() == null) {
                        errorMessages.add("pubsub sink module " + name + " parameter must contain protobufDescriptor when set format `protobuf`");
                    }
                    if(this.getProtobufMessageName() == null) {
                        errorMessages.add("pubsub sink module " + name + " parameter must contain protobufMessageName when set format `protobuf`");
                    }
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(this.attributes == null) {
                this.attributes = new ArrayList<>();
            }
            if(this.idAttributeFields == null) {
                this.idAttributeFields = new ArrayList<>();
            }
            if(this.orderingKeyFields == null) {
                this.orderingKeyFields = new ArrayList<>();
            } else if(!this.orderingKeyFields.isEmpty() && (this.maxBatchSize == null || this.maxBatchSize != 1)) {
                LOG.warn("pubsub sink module maxBatchSize must be 1 when using orderingKeyFields. ref: https://issues.apache.org/jira/browse/BEAM-13148");
                this.maxBatchSize = 1;
            }
        }

        public static PubSubSinkParameters of(final JsonObject jsonObject, final String name) {
            final PubSubSinkParameters parameters = new Gson().fromJson(jsonObject, PubSubSinkParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("pubsub sink module parameters must not be empty!");
            }
            parameters.validate(name);
            parameters.setDefaults();
            return parameters;
        }
    }

    private enum Format {
        avro,
        json,
        protobuf
    }

    @Override
    public String getName() { return "pubsub"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("pubsub sink module requires input parameter");
        }

        try {
            config.outputAvroSchema(inputs.get(0).getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for {} to path: {}", config.getName(), config.getOutputAvroSchema(), e);
        }

        final PubSubSinkParameters parameters = PubSubSinkParameters.of(config.getParameters(), config.getName());

        if(inputs.size() == 1) {
            final Write write = new Write(inputs.get(0), parameters, waits);
            inputs.get(0).getCollection().apply(config.getName(), write);
        } else {
            writeMulti(inputs, config, parameters, waits);
        }

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        final FCollection<?> collection = FCollection.update(inputs.get(0), config.getName (), (PCollection)inputs.get(0).getCollection());
        outputs.put(config.getName(), collection);
        return outputs;
    }

    private static PDone writeMulti(
            final List<FCollection<?>> inputs,
            final SinkConfig config,
            final PubSubSinkParameters parameters,
            final List<FCollection<?>> waits) {

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final Map<String, org.apache.beam.sdk.schemas.Schema> inputSchemas = new HashMap<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.put(input.getName(), input.getSchema());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final Schema inputSchema = inputs.get(0).getAvroSchema();
        final WriteMulti write = new WriteMulti(inputSchema, parameters, inputTags, inputNames, inputTypes, waits);
        return tuple.apply(config.getName(), write);
    }

    private static PubsubIO.Write<PubsubMessage> createWrite(PubSubSinkParameters parameters) {
        PubsubIO.Write<PubsubMessage> write;
        if(TemplateUtil.isTemplateText(parameters.getTopic())) {
            write = PubsubIO.writeMessagesDynamic().to(topicFunction);
        } else {
            write = PubsubIO.writeMessages().to(parameters.getTopic());
        }

        if (parameters.getIdAttribute() != null && parameters.getIdAttributeFields().isEmpty()) {
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
        return write;
    }

    private static final SerializableFunction<ValueInSingleWindow<PubsubMessage>,String> topicFunction = (ValueInSingleWindow<PubsubMessage> m) -> {
        if(m == null || m.getValue() == null) {
            throw new IllegalArgumentException();
        }
        if(m.getValue().getAttributeMap() == null || !m.getValue().getAttributeMap().containsKey(ATTRIBUTE_NAME_TOPIC)) {
            throw new IllegalArgumentException();
        }
        return m.getValue().getAttributeMap().get(ATTRIBUTE_NAME_TOPIC);
    };

    private static class Write extends PTransform<PCollection<?>, PDone> {

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

            final PubsubIO.Write<PubsubMessage> write = createWrite(parameters);
            switch (parameters.getFormat()) {
                case avro -> {
                    final PCollection<GenericRecord> records = input
                            .apply("ToRecord", DataTypeTransform.transform(collection, DataType.AVRO));
                    return records.apply("ToMessage", ParDo.of(new GenericRecordPubsubMessageDoFn(
                                    parameters.getAttributes(),
                                    parameters.getIdAttribute(),
                                    parameters.getIdAttributeFields(),
                                    parameters.getOrderingKeyFields(),
                                    collection.getAvroSchema().toString())))
                            .apply("PublishPubSub", write);
                }
                case json -> {
                    return switch (collection.getDataType()) {
                        case ROW -> {
                            final PCollection<Row> rows = (PCollection<Row>) input;
                            yield rows.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            RowSchemaUtil::getAsString,
                                            RowToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case AVRO -> {
                            final PCollection<GenericRecord> records = (PCollection<GenericRecord>) input;
                            yield records.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            AvroSchemaUtil::getAsString,
                                            RecordToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case STRUCT -> {
                            final PCollection<Struct> structs = (PCollection<Struct>) input;
                            yield structs.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            StructSchemaUtil::getAsString,
                                            StructToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case ENTITY -> {
                            final PCollection<Entity> entities = (PCollection<Entity>) input;
                            yield entities.apply("ToMessage", ParDo.of(new JsonPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            EntitySchemaUtil::getAsString,
                                            EntityToJsonConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        default ->
                                throw new IllegalArgumentException("PubSubSink module not support data type: " + collection.getDataType());
                    };
                }
                case protobuf -> {
                    return switch (collection.getDataType()) {
                        case ROW -> {
                            final PCollection<Row> rows = (PCollection<Row>) input;
                            yield rows.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            RowSchemaUtil::getAsString,
                                            RowToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case AVRO -> {
                            final PCollection<GenericRecord> records = (PCollection<GenericRecord>) input;
                            yield records.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            AvroSchemaUtil::getAsString,
                                            RecordToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case STRUCT -> {
                            final PCollection<Struct> structs = (PCollection<Struct>) input;
                            yield structs.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            StructSchemaUtil::getAsString,
                                            StructToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        case ENTITY -> {
                            final PCollection<Entity> entities = (PCollection<Entity>) input;
                            yield entities.apply("ToMessage", ParDo.of(new ProtobufPubsubMessageDoFn<>(
                                            parameters.getAttributes(),
                                            parameters.getIdAttribute(),
                                            parameters.getIdAttributeFields(),
                                            parameters.getOrderingKeyFields(),
                                            parameters.getProtobufDescriptor(),
                                            parameters.getProtobufMessageName(),
                                            EntitySchemaUtil::getAsString,
                                            EntityToProtoConverter::convert)))
                                    .apply("PublishPubSub", write);
                        }
                        default -> throw new IllegalArgumentException("PubSubSink module not support data type: " + collection.getDataType());
                    };
                }
                default -> throw new IllegalArgumentException("PubSubSink module not support format: " + parameters.getFormat());
            }
        }


        private static class JsonPubsubMessageDoFn<T> extends PubsubMessageDoFn<T> {

            private final SchemaUtil.StringGetter<T> stringGetter;
            private final JsonConverter<T> converter;

            JsonPubsubMessageDoFn(final List<String> attributes,
                                  final String idAttribute,
                                  final List<String> idAttributeFields,
                                  final List<String> orderingKeyFields,
                                  final SchemaUtil.StringGetter<T> stringGetter,
                                  final JsonConverter<T> converter) {

                super(attributes, idAttribute, idAttributeFields, orderingKeyFields);
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
                                           final List<String> idAttributeFields,
                                           final List<String> orderingKeyFields,
                                           final String schemaString) {

                super(attributes, idAttribute, idAttributeFields, orderingKeyFields);
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
                                      final List<String> idAttributeFields,
                                      final List<String> orderingKeyFields,
                                      final String descriptorPath,
                                      final String messageName,
                                      final SchemaUtil.StringGetter<T> stringGetter,
                                      final ProtoConverter<T> converter) {

                super(attributes, idAttribute, idAttributeFields, orderingKeyFields);
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
            private final List<String> idAttributeFields;
            private final List<String> orderingKeyFields;

            PubsubMessageDoFn(
                    final List<String> attributes,
                    final String idAttribute,
                    final List<String> idAttributeFields,
                    final List<String> orderingKeyFields) {

                this.attributes = attributes;
                this.idAttribute = idAttribute;
                this.idAttributeFields = idAttributeFields;
                this.orderingKeyFields = orderingKeyFields;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final Map<String, String> attributeMap = getAttributes(element);
                final String messageId = getMessageId(element);
                final String orderingKey = getOrderingKey(element);
                final byte[] payload = encode(element);
                if(this.idAttribute != null) {
                    attributeMap.put(this.idAttribute, messageId);
                }
                final PubsubMessage message = new PubsubMessage(payload, attributeMap, messageId, orderingKey);
                c.output(message);
            }

            private Map<String, String> getAttributes(T element) {
                final Map<String, String> attributeMap = new HashMap<>();
                if(attributes == null || attributes.isEmpty()) {
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
                if(idAttributeFields == null || idAttributeFields.isEmpty()) {
                    return null;
                }
                return getAttributesAsString(element, idAttributeFields);
            }

            private String getOrderingKey(T element) {
                if(orderingKeyFields == null || orderingKeyFields.isEmpty()) {
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
                if(!sb.isEmpty()) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                return sb.toString();
            }

            abstract String getString(T element, String fieldName);

            abstract byte[] encode(T element);

        }
    }

    private static class WriteMulti extends PTransform<PCollectionTuple, PDone> {

        private static final Logger LOG = LoggerFactory.getLogger(Write.class);

        private final Schema schema;
        private final PubSubSinkParameters parameters;

        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        private final List<FCollection<?>> waitCollections;

        private WriteMulti(
                final Schema schema,
                final PubSubSinkParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final List<FCollection<?>> waits) {

            this.schema = schema;
            this.parameters = parameters;
            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.waitCollections = waits;
        }

        public PDone expand(final PCollectionTuple inputs) {

            final PCollection<UnionValue> unionValues = inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames));

            final PCollection<UnionValue> waited;
            if(waitCollections == null) {
                waited = unionValues;
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = unionValues
                        .apply("Wait", Wait.on(waits))
                        .setCoder(unionValues.getCoder());
            }

            final PubsubIO.Write<PubsubMessage> write = createWrite(parameters);

            if(TemplateUtil.isTemplateText(parameters.getTopic())) {
                return waited
                        .apply("ToMessage", ParDo.of(new UnionPubsubMessageDoFn(parameters, schema.toString())))
                        .apply("PublishPubSub", write);
            }

            return waited
                    .apply("ToMessage", ParDo.of(new UnionPubsubMessageDoFn(parameters, schema.toString())))
                    .apply("PublishPubSub", write);
        }

        private static class UnionPubsubMessageDoFn extends DoFn<UnionValue, PubsubMessage> {

            private final Format format;
            private final List<String> attributes;
            private final String idAttribute;
            private final List<String> idAttributeFields;
            private final List<String> orderingKeyFields;
            private final String topic;
            private final boolean isDynamicTopic;

            private final String schemaString;
            private final String descriptorPath;
            private final String messageName;

            private final List<String> topicTemplateArgs;

            private transient Schema schema;
            private transient DatumWriter<GenericRecord> writer;
            private transient BinaryEncoder encoder;
            private transient Descriptors.Descriptor descriptor;

            private transient Template topicTemplate;


            UnionPubsubMessageDoFn(
                    final PubSubSinkParameters parameters,
                    final String schemaString) {

                this.format = parameters.getFormat();
                this.attributes = parameters.getAttributes();
                this.idAttribute = parameters.getIdAttribute();
                this.idAttributeFields = parameters.getIdAttributeFields();
                this.orderingKeyFields = parameters.getOrderingKeyFields();
                this.topic = parameters.getTopic();
                this.isDynamicTopic = TemplateUtil.isTemplateText(parameters.getTopic());

                this.schemaString = schemaString;
                this.descriptorPath = parameters.getProtobufDescriptor();
                this.messageName = parameters.getProtobufMessageName();

                final Schema avroSchema = AvroSchemaUtil.convertSchema(schemaString);
                this.topicTemplateArgs = TemplateUtil.extractTemplateArgs(this.topic, avroSchema);
            }

            @Setup
            public void setup() {
                switch (format) {
                    case avro -> {
                        this.schema = AvroSchemaUtil.convertSchema(schemaString);
                        this.writer = new GenericDatumWriter<>(schema);
                        this.encoder = null;
                    }
                    case protobuf -> {
                        final byte[] bytes = StorageUtil.readBytes(descriptorPath);
                        final Map<String, Descriptors.Descriptor> descriptors = ProtoSchemaUtil.getDescriptors(bytes);
                        this.descriptor = descriptors.get(messageName);
                    }
                }
                if(isDynamicTopic) {
                    this.topicTemplate = TemplateUtil.createStrictTemplate("topicTemplate", topic);
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnionValue element = c.element();
                final Map<String, String> attributeMap = getAttributes(element);
                final String messageId = getMessageId(element);
                final String orderingKey = getOrderingKey(element);
                final byte[] payload = switch (format) {
                    case json -> {
                        final String json = UnionValue.getAsJson(element);
                        yield Optional.ofNullable(json).map(s -> s.getBytes(StandardCharsets.UTF_8)).orElse(null);
                    }
                    case avro -> {
                        final GenericRecord record = UnionValue.getAsRecord(schema, element);
                        yield encode(record);
                    }
                    case protobuf -> {
                        final DynamicMessage message = UnionValue.getAsProtoMessage(descriptor, element);
                        yield Optional.ofNullable(message).map(DynamicMessage::toByteArray).orElse(null);
                    }
                };
                if(this.idAttribute != null) {
                    attributeMap.put(this.idAttribute, messageId);
                }
                final PubsubMessage message = new PubsubMessage(payload, attributeMap, messageId, orderingKey);
                c.output(message);
            }

            private Map<String, String> getAttributes(UnionValue element) {
                final Map<String, String> attributeMap = new HashMap<>();
                if(isDynamicTopic) {
                    final String topic = TemplateUtil.executeStrictTemplate(topicTemplate, element.getMap(topicTemplateArgs));
                    attributeMap.put(ATTRIBUTE_NAME_TOPIC, topic);
                }
                if(attributes == null || attributes.isEmpty()) {
                    return attributeMap;
                }
                for(final String attribute : attributes) {
                    final String value = element.getString(attribute);
                    if(value == null) {
                        continue;
                    }
                    attributeMap.put(attribute, value);
                }
                return attributeMap;
            }

            private String getMessageId(UnionValue element) {
                if(idAttributeFields == null || idAttributeFields.isEmpty()) {
                    return null;
                }
                return getAttributesAsString(element, idAttributeFields);
            }

            private String getOrderingKey(UnionValue element) {
                if(orderingKeyFields == null || orderingKeyFields.isEmpty()) {
                    return null;
                }
                return getAttributesAsString(element, orderingKeyFields);
            }

            private String getAttributesAsString(final UnionValue value, final List<String> fields) {
                final StringBuilder sb = new StringBuilder();
                for(final String fieldName : fields) {
                    final String fieldValue = value.getString(fieldName);
                    sb.append(fieldValue == null ? "" : fieldValue);
                    sb.append("#");
                }
                if(!sb.isEmpty()) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                return sb.toString();
            }

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

    }

    private interface JsonConverter<T> extends Serializable {
        String convert(final T value);
    }

    private interface ProtoConverter<T> extends Serializable {
        DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final T value);
    }

}