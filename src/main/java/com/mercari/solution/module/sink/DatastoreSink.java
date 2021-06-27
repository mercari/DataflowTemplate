package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import freemarker.template.Template;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


public class DatastoreSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSink.class);

    private class DatastoreSinkParameters implements Serializable {

        private String projectId;
        private String kind;
        private List<String> keyFields;
        private String keyTemplate;
        private Boolean delete;

        private String separator;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public void setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields;
        }

        public String getKeyTemplate() {
            return keyTemplate;
        }

        public void setKeyTemplate(String keyTemplate) {
            this.keyTemplate = keyTemplate;
        }

        public Boolean getDelete() {
            return delete;
        }

        public void setDelete(Boolean delete) {
            this.delete = delete;
        }

        public String getSeparator() {
            return separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }
    }

    public String getName() { return "datastore"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), DatastoreSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> input, final SinkConfig config, final List<FCollection<?>> waitCollections) {

        final DatastoreSinkParameters parameters = new Gson().fromJson(config.getParameters(), DatastoreSinkParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        try {
            config.outputAvroSchema(input.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

        final PDone output;
        switch (input.getDataType()) {
            case AVRO: {
                final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                final Write<GenericRecord, String, Schema> write = new Write<>(
                        parameters,
                        input.getAvroSchema().toString(),
                        AvroSchemaUtil::convertSchema,
                        AvroSchemaUtil::getAsString,
                        RecordToMapConverter::convert,
                        RecordToEntityConverter::convertBuilder,
                        waitCollections);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            case ROW: {
                final FCollection<Row> inputCollection = (FCollection<Row>) input;
                final Write<Row, org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema> write = new Write<>(
                        parameters,
                        input.getSchema(),
                        (org.apache.beam.sdk.schemas.Schema s) -> s,
                        RowSchemaUtil::getAsString,
                        RowToMapConverter::convert,
                        RowToEntityConverter::convertBuilder,
                        waitCollections);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            case STRUCT: {
                final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                final Write<Struct, Type, Type> write = new Write<>(
                        parameters,
                        input.getSpannerType(),
                        s -> s,
                        StructSchemaUtil::getAsString,
                        StructToMapConverter::convert,
                        StructToEntityConverter::convertBuilder,
                        waitCollections);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            case ENTITY: {
                final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                final Write<Entity, org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema> write = new Write<>(
                        parameters,
                        input.getSchema(),
                        s -> s,
                        EntitySchemaUtil::getAsString,
                        EntityToMapConverter::convert,
                        (s, e) -> e.toBuilder(),
                        waitCollections);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
        }
        return FCollection.update(input, config.getName(), (PCollection) input.getCollection());
    }

    private static void validateParameters(DatastoreSinkParameters parameters) {
        if(parameters.getProjectId() == null) {
            throw new IllegalArgumentException("Datastore output module requires projectId parameter!");
        }
    }

    private static void setDefaultParameters(DatastoreSinkParameters parameters) {
        if(parameters.getKeyFields() == null) {
            parameters.setKeyFields(new ArrayList<>());
        }
        if(parameters.getDelete() == null) {
            parameters.setDelete(false);
        }
        if(parameters.getSeparator() == null) {
            parameters.setSeparator("#");
        }
    }

    public static class Write<T,InputSchema,RuntimeSchema> extends PTransform<PCollection<T>, PDone> {

        private final DatastoreSinkParameters parameters;
        private final List<FCollection<?>> waitCollections;

        private final InputSchema inputSchema;
        private final SchemaConverter<InputSchema,RuntimeSchema> schemaConverter;
        private final StringGetter<T> stringGetter;
        private final MapConverter<T> mapConverter;
        private final EntityConverter<T,RuntimeSchema> entityConverter;

        private Write(final DatastoreSinkParameters parameters,
                      final InputSchema inputSchema,
                      final SchemaConverter<InputSchema, RuntimeSchema> schemaConverter,
                      final StringGetter<T> stringGetter,
                      final MapConverter<T> mapConverter,
                      final EntityConverter<T, RuntimeSchema> entityConverter,
                      final List<FCollection<?>> waitCollections) {

            this.parameters = parameters;
            this.waitCollections = waitCollections;

            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.stringGetter = stringGetter;
            this.mapConverter = mapConverter;
            this.entityConverter = entityConverter;
        }

        public PDone expand(final PCollection<T> input) {

            final String execEnvProject = input.getPipeline().getOptions().as(GcpOptions.class).getProject();

            final PCollection<Entity> entities;
            entities = input.apply("ToEntity", ParDo.of(new EntityDoFn<>(
                    inputSchema,
                    parameters.getKind(), parameters.getKeyFields(), parameters.getKeyTemplate(), parameters.getSeparator(),
                    schemaConverter, stringGetter, mapConverter, entityConverter)));

            if(parameters.getDelete()) {
                final DatastoreV1.DeleteEntity delete = DatastoreIO.v1().deleteEntity()
                        .withProjectId(Optional.ofNullable(parameters.getProjectId()).orElse(execEnvProject));
                if(waitCollections == null) {
                    return entities.apply("DeleteEntity", delete);
                } else {
                    final List<PCollection<?>> waits = waitCollections.stream()
                            .map(f -> f.getCollection())
                            .collect(Collectors.toList());
                    return entities
                            .apply("Wait", Wait.on(waits))
                            .setCoder(entities.getCoder())
                            .apply("DeleteEntity", delete);
                }
            } else {
                final DatastoreV1.Write write = DatastoreIO.v1().write()
                        .withProjectId(Optional.ofNullable(parameters.getProjectId()).orElse(execEnvProject));
                if(waitCollections == null) {
                    return entities.apply("WriteEntity", write);
                } else {
                    final List<PCollection<?>> waits = waitCollections.stream()
                            .map(f -> f.getCollection())
                            .collect(Collectors.toList());
                    return entities
                            .apply("Wait", Wait.on(waits))
                            .setCoder(entities.getCoder())
                            .apply("WriteEntity", write);
                }
            }
        }

    }

    private static class EntityDoFn<T,InputSchema,RuntimeSchema> extends DoFn<T, Entity> {

        private static final DateTimeUtil.DateTimeTemplateUtils datetimeUtils = new DateTimeUtil.DateTimeTemplateUtils();

        private final InputSchema inputSchema;
        private final String kind;
        private final List<String> keyFields;
        private final String keyTemplate;

        private final String separator;

        private final SchemaConverter<InputSchema,RuntimeSchema> schemaConverter;
        private final StringGetter<T> stringGetter;
        private final MapConverter<T> mapConverter;
        private final EntityConverter<T,RuntimeSchema> entityConverter;

        private transient RuntimeSchema schema;
        private transient Template templateKey;

        public EntityDoFn(final InputSchema inputSchema,
                          final String kind,
                          final List<String> keyFields,
                          final String keyTemplate,
                          final String separator,
                          final SchemaConverter<InputSchema,RuntimeSchema> schemaConverter,
                          final StringGetter<T> stringGetter,
                          final MapConverter<T> mapConverter,
                          final EntityConverter<T,RuntimeSchema> entityConverter) {

            this.inputSchema = inputSchema;
            this.kind = kind;
            this.keyFields = keyFields;
            this.keyTemplate = keyTemplate;
            this.separator = separator;
            this.schemaConverter = schemaConverter;
            this.stringGetter = stringGetter;
            this.mapConverter = mapConverter;
            this.entityConverter = entityConverter;
        }

        @Setup
        public void setup() {
            this.schema = schemaConverter.convert(inputSchema);
            if(keyTemplate != null) {
                this.templateKey = TemplateUtil.createStrictTemplate("keyTemplate", keyTemplate);
            } else {
                this.templateKey = null;
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final T element = c.element();
            final Entity.Builder builder = entityConverter.convert(schema, element);

            // Generate key
            final com.google.datastore.v1.Key key;
            if(keyFields != null && keyFields.size() == 1
                    && builder.containsProperties(keyFields.get(0))
                    && builder.getPropertiesOrThrow(keyFields.get(0)).getValueTypeCase().equals(Value.ValueTypeCase.INTEGER_VALUE)) {

                final com.google.datastore.v1.Key.PathElement pathElement = com.google.datastore.v1.Key.PathElement
                        .newBuilder()
                        .setKind(kind)
                        .setId(builder.getPropertiesOrThrow(keyFields.get(0)).getIntegerValue())
                        .build();
                key = com.google.datastore.v1.Key
                        .newBuilder()
                        .addPath(pathElement)
                        .build();;
            } else if((keyFields != null && keyFields.size() > 0) || templateKey != null) {
                final String keyString;
                if(keyFields != null && keyFields.size() > 0) {
                    final StringBuilder sb = new StringBuilder();
                    for (final String keyField : keyFields) {
                        final String keyValue = stringGetter.getAsString(element, keyField);
                        sb.append(keyValue);
                        sb.append(separator);
                    }
                    sb.deleteCharAt(sb.length() - separator.length());
                    keyString = sb.toString();
                } else {
                    final Map<String,Object> data = mapConverter.convert(element);
                    data.put("_DateTimeUtil", datetimeUtils);
                    data.put("_EVENTTIME", Instant.ofEpochMilli(c.timestamp().getMillis()));
                    keyString = TemplateUtil.executeStrictTemplate(templateKey, data);
                }

                final com.google.datastore.v1.Key.PathElement pathElement = com.google.datastore.v1.Key.PathElement
                        .newBuilder()
                        .setKind(kind)
                        .setName(keyString)
                        .build();
                key = com.google.datastore.v1.Key
                        .newBuilder()
                        .addPath(pathElement)
                        .build();
            } else if(builder.getKey() != null) {
                final int index = builder.getKey().getPathCount() - 1;
                final com.google.datastore.v1.Key.PathElement pathElement = builder
                        .getKey()
                        .getPathList()
                        .get(index)
                        .toBuilder()
                        .setKind(kind)
                        .build();
                key = builder.getKey().toBuilder().setPath(index, pathElement).build();
            } else {
                final String keyString = UUID.randomUUID().toString();
                final com.google.datastore.v1.Key.PathElement pathElement = com.google.datastore.v1.Key.PathElement
                        .newBuilder()
                        .setKind(kind)
                        .setName(keyString)
                        .build();
                key = com.google.datastore.v1.Key
                        .newBuilder()
                        .addPath(pathElement)
                        .build();
            }

            final Entity entity = builder.setKey(key).build();
            c.output(entity);
        }

    }

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(InputSchemaT schema);
    }

    private interface EntityConverter<InputT, RuntimeSchemaT> extends Serializable {
        Entity.Builder convert(RuntimeSchemaT schema, InputT element);
    }

    private interface MapConverter<T> extends Serializable {
        Map<String, Object> convert(T element);
    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(T element, String field);
    }


}
