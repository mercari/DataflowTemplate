package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Write;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.FirestoreUtil;
import com.mercari.solution.util.schema.*;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class FirestoreSink implements SinkModule {

    private static final String NAME_FIELD = "__name__";

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreSink.class);

    private class FirestoreSinkParameters implements Serializable {

        private String projectId;
        private String databaseId;
        private String collection;
        private List<String> nameFields;
        private String nameTemplate;
        private Boolean delete;
        private Boolean failFast;
        private Boolean shuffle;
        private String separator;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getDatabaseId() {
            return databaseId;
        }

        public void setDatabaseId(String databaseId) {
            this.databaseId = databaseId;
        }

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }

        public List<String> getNameFields() {
            return nameFields;
        }

        public void setNameFields(List<String> nameFields) {
            this.nameFields = nameFields;
        }

        public String getNameTemplate() {
            return nameTemplate;
        }

        public void setNameTemplate(String nameTemplate) {
            this.nameTemplate = nameTemplate;
        }

        public Boolean getDelete() {
            return delete;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public void setFailFast(Boolean failFast) {
            this.failFast = failFast;
        }

        public void setDelete(Boolean delete) {
            this.delete = delete;
        }

        public Boolean getShuffle() {
            return shuffle;
        }

        public void setShuffle(Boolean shuffle) {
            this.shuffle = shuffle;
        }

        public String getSeparator() {
            return separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }

        private void validate() {
            if((this.collection == null || this.nameFields == null) && this.nameTemplate == null) {
                throw new IllegalArgumentException("Firestore sink module requires name parameter!");
            }
        }

        private void setDefaults(final String defaultProjectId) {
            if(this.projectId == null) {
                this.projectId = defaultProjectId;
            }
            if(this.databaseId == null) {
                this.databaseId = "(default)";
            }
            if(this.delete == null) {
                setDelete(false);
            }
            if(this.failFast == null) {
                this.failFast = true;
            }
            if(this.shuffle == null) {
                this.shuffle = true;
            }
            if(this.separator == null) {
                this.setSeparator("#");
            }
        }
    }

    public String getName() { return "firestore"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("firestore sink module requires input parameter");
        }
        final FCollection<?> input = inputs.get(0);
        return Collections.singletonMap(config.getName(), write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waitCollections) {
        final FirestoreSinkParameters parameters = new Gson().fromJson(config.getParameters(), FirestoreSinkParameters.class);
        parameters.validate();
        final String defaultProject = OptionUtil.getProject(collection.getCollection());
        parameters.setDefaults(defaultProject);

        final DataType inputType = collection.getDataType();
        switch (inputType) {
            case ROW: {
                final Schema inputSchema = collection.getSchema();
                final FirestoreWrite<Schema,Schema,Row> write = new FirestoreWrite<>(
                        parameters,
                        inputSchema,
                        s -> s,
                        RowToDocumentConverter::convert,
                        RowSchemaUtil::getAsString,
                        RowToMapConverter::convert,
                        waitCollections);
                final PCollection<Row> input = (PCollection<Row>) collection.getCollection();
                final PCollection<FirestoreV1.WriteSuccessSummary> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.ROW, collection.getSchema());
            }
            case AVRO: {
                final org.apache.avro.Schema inputSchema = collection.getAvroSchema();
                final FirestoreWrite<String,org.apache.avro.Schema,GenericRecord> write = new FirestoreWrite<>(
                        parameters,
                        inputSchema.toString(),
                        AvroSchemaUtil::convertSchema,
                        RecordToDocumentConverter::convert,
                        AvroSchemaUtil::getAsString,
                        RecordToMapConverter::convert,
                        waitCollections);
                final PCollection<GenericRecord> input = (PCollection<GenericRecord>) collection.getCollection();
                final PCollection<FirestoreV1.WriteSuccessSummary> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, collection.getAvroSchema());
            }
            case STRUCT: {
                final Type inputSpannerType = collection.getSpannerType();
                final FirestoreWrite<Type,Type,Struct> write = new FirestoreWrite<>(
                        parameters,
                        inputSpannerType,
                        t -> t,
                        StructToDocumentConverter::convert,
                        StructSchemaUtil::getAsString,
                        StructToMapConverter::convert,
                        waitCollections);
                final PCollection<Struct> input = (PCollection<Struct>) collection.getCollection();
                final PCollection<FirestoreV1.WriteSuccessSummary> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.STRUCT, collection.getSpannerType());
            }
            case ENTITY: {
                final Schema inputSchema = collection.getSchema();
                final FirestoreWrite<Schema,Schema,Entity> write = new FirestoreWrite<>(
                        parameters,
                        inputSchema,
                        s -> s,
                        EntityToDocumentConverter::convert,
                        EntitySchemaUtil::getAsString,
                        EntityToMapConverter::convert,
                        waitCollections);
                final PCollection<Entity> input = (PCollection<Entity>) collection.getCollection();
                final PCollection<FirestoreV1.WriteSuccessSummary> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.ENTITY, collection.getSchema());
            }
            case DOCUMENT: {
                final Schema inputSchema = collection.getSchema();
                final FirestoreWrite<Schema,Schema,Document> write = new FirestoreWrite<>(
                        parameters,
                        inputSchema,
                        s -> s,
                        DocumentSchemaUtil::toBuilder,
                        DocumentSchemaUtil::getAsString,
                        DocumentToMapConverter::convert,
                        waitCollections);
                final PCollection<Document> input = (PCollection<Document>) collection.getCollection();
                final PCollection<FirestoreV1.WriteSuccessSummary> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.DOCUMENT, collection.getSchema());
            }
            default: {
                throw new IllegalArgumentException("Not supported ");
            }
        }
    }

    public static class FirestoreWrite<SchemaInputT, SchemaRuntimeT, T> extends PTransform<PCollection<T>, PCollection<FirestoreV1.WriteSuccessSummary>> {

        private final SchemaInputT inputSchema;
        private final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter;
        private final DocumentConverter<SchemaRuntimeT, T> converter;
        private final SchemaUtil.StringGetter<T> stringGetter;
        private final SchemaUtil.MapConverter<T> mapConverter;
        private final FirestoreSinkParameters parameters;
        private final List<FCollection<?>> waitCollections;

        private FirestoreWrite(final FirestoreSinkParameters parameters,
                               final SchemaInputT inputSchema,
                               final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter,
                               final DocumentConverter<SchemaRuntimeT, T> converter,
                               final SchemaUtil.StringGetter<T> stringGetter,
                               final SchemaUtil.MapConverter<T> mapConverter,
                               final List<FCollection<?>> waitCollections) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.converter = converter;
            this.stringGetter = stringGetter;
            this.mapConverter = mapConverter;
            this.waitCollections = waitCollections;
        }

        public PCollection<FirestoreV1.WriteSuccessSummary> expand(final PCollection<T> input) {

            final PCollection<T> waited;
            if(waitCollections != null && waitCollections.size() > 0) {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = input
                        .apply("Wait", Wait.on(waits))
                        .setCoder(input.getCoder());
            } else {
                waited = input;
            }

            final PCollection<Write> writes;
            if(parameters.getShuffle()) {
                writes = waited
                        .apply("ConvertToDocument", ParDo.of(new ConvertWriteDoFn(
                                parameters, inputSchema, schemaConverter, converter, stringGetter, mapConverter)));
            } else {
                writes = waited
                        .apply("ConvertToDocument", ParDo.of(new ConvertWriteDoFn(
                                parameters, inputSchema, schemaConverter, converter, stringGetter, mapConverter)))
                        .apply("Reshuffle", Reshuffle.viaRandomKey());
            }


            if(parameters.getFailFast()) {
                return writes
                        .apply("Reshuffle", Reshuffle.viaRandomKey())
                        .apply("WriteDocument", FirestoreIO.v1().write().batchWrite().build());
            } else {
                return writes
                        .apply("WriteDocument", FirestoreIO.v1().write().batchWrite().build());
            }

        }

        private class ConvertWriteDoFn extends DoFn<T, Write> {

            private final SchemaInputT inputSchema;
            private final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter;
            private final DocumentConverter<SchemaRuntimeT, T> converter;
            private final SchemaUtil.StringGetter<T> stringGetter;
            private final SchemaUtil.MapConverter<T> mapConverter;

            private final String project;
            private final String database;
            private final String collection;
            private final List<String> nameFields;
            private final String nameTemplateText;
            private final boolean delete;
            private final String separator;

            private transient SchemaRuntimeT runtimeSchema;
            private transient Template nameTemplate;

            ConvertWriteDoFn(
                    final FirestoreSinkParameters parameters,
                    final SchemaInputT inputSchema,
                    final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter,
                    final DocumentConverter<SchemaRuntimeT, T> converter,
                    final SchemaUtil.StringGetter<T> stringGetter,
                    final SchemaUtil.MapConverter<T> mapConverter) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.converter = converter;
                this.stringGetter = stringGetter;
                this.mapConverter = mapConverter;

                this.project = parameters.getProjectId();
                this.database = parameters.getDatabaseId();
                this.collection = parameters.getCollection();
                this.nameFields = parameters.getNameFields();
                this.nameTemplateText = parameters.getNameTemplate();
                this.delete = parameters.getDelete();
                this.separator = parameters.getSeparator();
            }

            @Setup
            public void setup() {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
                if(nameTemplateText != null) {
                    this.nameTemplate = TemplateUtil.createStrictTemplate("firestoreSinkNameTemplate", nameTemplateText);
                } else {
                    this.nameTemplate = null;
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T input = c.element();

                final String name;
                if((nameFields == null || nameFields.size() == 0) && nameTemplate == null) {
                    final String defaultNameValue = stringGetter.getAsString(input, NAME_FIELD);
                    if(defaultNameValue == null) {
                        name = createName(UUID.randomUUID().toString());
                    } else {
                        name = defaultNameValue;
                    }
                } else if(nameTemplate == null) {
                    final StringBuilder sb = new StringBuilder();
                    for (final String nameField : nameFields) {
                        final String nameValue = stringGetter.getAsString(input, nameField);
                        sb.append(nameValue);
                        sb.append(separator);
                    }
                    sb.deleteCharAt(sb.length() - separator.length());
                    name = createName( sb.toString());
                } else {
                    final Map<String, Object> data = mapConverter.convert(input);
                    final String path = TemplateUtil.executeStrictTemplate(nameTemplate, data);
                    name = createName(path);
                }

                if(delete) {
                    final Write delete = Write.newBuilder()
                            .setDelete(name)
                            .build();
                    c.output(delete);
                } else {
                    final Document document = converter
                            .convert(runtimeSchema, input)
                            .setName(name)
                            .build();
                    final Write write = Write.newBuilder()
                            .setUpdate(document)
                            .build();
                    c.output(write);
                }
            }
            private String createName(final String nameString) {
                if(collection == null) {
                    return FirestoreUtil.createName(project, database, nameString);
                } else {
                    return FirestoreUtil.createName(project, database, collection, nameString);
                }
            }
        }

    }

    private interface DocumentConverter<SchemaT,T> extends Serializable {
        Document.Builder convert(final SchemaT schema, final T record);
    }

}
