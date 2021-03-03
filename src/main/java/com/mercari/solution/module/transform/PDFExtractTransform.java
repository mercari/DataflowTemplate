package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.NullValue;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.text.PDFTextStripper;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

public class PDFExtractTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(PDFExtractTransform.class);

    private static final String FIELD_NAME_CONTENT = "Content";
    private static final String FIELD_NAME_PAGE = "Page";
    private static final String FIELD_NAME_FILESIZE = "FileByteSize";
    private static final String FIELD_NAME_TITLE = "Title";
    private static final String FIELD_NAME_AUTHOR = "Author";
    private static final String FIELD_NAME_SUBJECT = "Subject";
    private static final String FIELD_NAME_KEYWORDS = "Keywords";
    private static final String FIELD_NAME_CREATOR = "Creator";
    private static final String FIELD_NAME_PRODUCER = "Producer";
    private static final String FIELD_NAME_CREATIONDATE = "CreationDate";
    private static final String FIELD_NAME_MODIFICATIONDATE = "ModificationDate";
    private static final String FIELD_NAME_TRAPPED = "Trapped";

    private class PDFExtractTransformParameters {

        private String field;
        private String prefix;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }
    }

    public String getName() { return "pdfextract"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return PDFExtractTransform.transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final PDFExtractTransformParameters parameters = new Gson().fromJson(config.getParameters(), PDFExtractTransformParameters.class);
        validate(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> collections = new HashMap<>();
        for(final FCollection input : inputs) {
            final String name = config.getName() + "." + input.getName();

            final String contentFieldName = parameters.getPrefix() + FIELD_NAME_CONTENT;
            final String pageFieldName = parameters.getPrefix() + FIELD_NAME_PAGE;
            final String fileSizeFieldName = parameters.getPrefix() + FIELD_NAME_FILESIZE;
            final String titleFieldName = parameters.getPrefix() + FIELD_NAME_TITLE;
            final String authorFieldName = parameters.getPrefix() + FIELD_NAME_AUTHOR;
            final String subjectFieldName = parameters.getPrefix() + FIELD_NAME_SUBJECT;
            final String keywordsFieldName = parameters.getPrefix() + FIELD_NAME_KEYWORDS;
            final String creatorFieldName = parameters.getPrefix() + FIELD_NAME_CREATOR;
            final String producerFieldName = parameters.getPrefix() + FIELD_NAME_PRODUCER;
            final String creationDateFieldName = parameters.getPrefix() + FIELD_NAME_CREATIONDATE;
            final String modificationDateFieldName = parameters.getPrefix() + FIELD_NAME_MODIFICATIONDATE;
            final String trappedFieldName = parameters.getPrefix() + FIELD_NAME_TRAPPED;

            final Schema outputAvroSchema = AvroSchemaUtil
                    .toBuilder(input.getAvroSchema(), input.getAvroSchema().getNamespace(), null)
                    .name(contentFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(pageFieldName).type(AvroSchemaUtil.NULLABLE_INT).noDefault()
                    .name(fileSizeFieldName).type(AvroSchemaUtil.NULLABLE_INT).noDefault()
                    .name(titleFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(authorFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(subjectFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(keywordsFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(creatorFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(producerFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name(creationDateFieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                    .name(modificationDateFieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                    .name(trappedFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .endRecord();

            switch (input.getDataType()) {
                case ROW: {
                    final org.apache.beam.sdk.schemas.Schema outputSchema = RowSchemaUtil.addSchema(
                            input.getSchema(), Arrays.asList(
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(contentFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(pageFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.INT32),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(fileSizeFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.INT32),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(titleFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(authorFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(subjectFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(keywordsFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(creatorFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(producerFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(creationDateFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(modificationDateFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME),
                                    org.apache.beam.sdk.schemas.Schema.Field.nullable(trappedFieldName,org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
                                    ));
                    final PDFExtract transform = new PDFExtract<>(config,
                            (Row row, String field) -> {
                                if (!row.getSchema().hasField(field)) {
                                    return null;
                                }
                                if(row.getValue(field) == null) {
                                    return null;
                                }
                                final org.apache.beam.sdk.schemas.Schema.Field sfield = row.getSchema().getField(field);
                                switch (sfield.getType().getTypeName()) {
                                    case STRING: {
                                        final String path = row.getString(field);
                                        if (path.startsWith("gs://")) {
                                            return StorageUtil.readBytes(path);
                                        } else if(path.startsWith("/gs/")) {
                                            return StorageUtil.readBytes(path.replaceFirst("/gs/", "gs://"));
                                        }
                                        return null;
                                    }
                                    case BYTES:
                                        return row.getBytes(field);
                                    default:
                                        throw new IllegalArgumentException("pdfField must be String or Bytes. " + sfield.getType().getTypeName());
                                }
                            },
                            (Row row, org.apache.beam.sdk.schemas.Schema schema, String prefix, PDFContent content) -> {
                                final String text;
                                if (content.getTexts() != null && content.getTexts().size() > 0) {
                                    text = String.join(" ", content.getTexts());
                                } else {
                                    text = null;
                                }

                                return Row
                                        .fromRow(row)
                                        .withFieldValue(prefix + FIELD_NAME_CONTENT, text)
                                        .withFieldValue(prefix + FIELD_NAME_PAGE, content.getPage())
                                        .withFieldValue(prefix + FIELD_NAME_FILESIZE, content.getFileSize())
                                        .withFieldValue(prefix + FIELD_NAME_TITLE, content.getInformation().getTitle())
                                        .withFieldValue(prefix + FIELD_NAME_AUTHOR, content.getInformation().getAuthor())
                                        .withFieldValue(prefix + FIELD_NAME_SUBJECT, content.getInformation().getSubject())
                                        .withFieldValue(prefix + FIELD_NAME_KEYWORDS, content.getInformation().getKeywords())
                                        .withFieldValue(prefix + FIELD_NAME_CREATOR, content.getInformation().getCreator())
                                        .withFieldValue(prefix + FIELD_NAME_PRODUCER, content.getInformation().getProducer())
                                        .withFieldValue(prefix + FIELD_NAME_CREATIONDATE, content.getInformation().getCreationDate())
                                        .withFieldValue(prefix + FIELD_NAME_MODIFICATIONDATE, content.getInformation().getModificationDate())
                                        .withFieldValue(prefix + FIELD_NAME_TRAPPED, content.getInformation().getTrapped())
                                        .build();
                            },
                            outputSchema,
                            s -> s);
                    final Coder coder = RowCoder.of(outputSchema);
                    final PCollection<?> output = ((PCollection<?>) (input.getCollection())
                            .apply(config.getName(), transform))
                            .setCoder(coder);
                    collections.put(name, FCollection.of(name, output, DataType.ROW, outputSchema));
                    break;
                }
                case AVRO: {
                    final PDFExtract transform = new PDFExtract<>(config,
                            (GenericRecord record, String field) -> {
                                if (record.getSchema().getField(field) == null) {
                                    return null;
                                }
                                final Object value = record.get(field);
                                if(value == null) {
                                    return null;
                                }
                                final Schema.Type stype = AvroSchemaUtil.unnestUnion(record.getSchema().getField(field).schema()).getType();
                                switch (stype) {
                                    case STRING: {
                                        if (value.toString().startsWith("gs://")) {
                                            LOG.error("readBytes from: " + value.toString());
                                            byte[] bytes = StorageUtil.readBytes(value.toString());
                                            if(bytes == null) {
                                                LOG.error("Bytes is null: " + value.toString());
                                                return null;
                                            }
                                            return bytes;
                                        } else if(value.toString().startsWith("/gs/")) {
                                            final String path = value.toString().replaceFirst("/gs/", "gs://");
                                            LOG.error("readBytes from: " + path);
                                            byte[] bytes =  StorageUtil.readBytes(path);
                                            if(bytes == null) {
                                                LOG.error("Bytes is null: " + path);
                                                return null;
                                            }
                                            return bytes;
                                        }
                                        throw new IllegalArgumentException("Must not be null");
                                    }
                                    case BYTES:
                                        return ((ByteBuffer) value).array();
                                    default:
                                        throw new IllegalArgumentException("pdfField must be String or Bytes. " + stype);
                                }
                            },
                            (GenericRecord record, Schema schema, String prefix, PDFContent content) -> {
                                final String text;
                                if (content.getTexts() != null && content.getTexts().size() > 0) {
                                    text = String.join(" ", content.getTexts());
                                } else {
                                    text = null;
                                }
                                final PDFDocumentInformation info = content.getInformation();
                                return AvroSchemaUtil
                                        .toBuilder(record, schema)
                                        .set(prefix + FIELD_NAME_CONTENT, text)
                                        .set(prefix + FIELD_NAME_PAGE, content.getPage())
                                        .set(prefix + FIELD_NAME_FILESIZE, content.getFileSize())
                                        .set(prefix + FIELD_NAME_TITLE, info.getTitle())
                                        .set(prefix + FIELD_NAME_AUTHOR, info.getAuthor())
                                        .set(prefix + FIELD_NAME_SUBJECT, info.getSubject())
                                        .set(prefix + FIELD_NAME_KEYWORDS, info.getKeywords())
                                        .set(prefix + FIELD_NAME_CREATOR, info.getCreator())
                                        .set(prefix + FIELD_NAME_PRODUCER, info.getProducer())
                                        .set(prefix + FIELD_NAME_CREATIONDATE, info.getCreationDate() == null ? null : info.getCreationDate().getMillis() * 1000)
                                        .set(prefix + FIELD_NAME_MODIFICATIONDATE, info.getModificationDate() == null ? null : info.getModificationDate().getMillis() * 1000)
                                        .set(prefix + FIELD_NAME_TRAPPED, info.getTrapped())
                                        .build();
                            },
                            outputAvroSchema.toString(),
                            AvroSchemaUtil::convertSchema);
                    final Coder coder = AvroCoder.of(outputAvroSchema);
                    final PCollection<?> output = ((PCollection<?>) (input.getCollection())
                            .apply(config.getName(), transform))
                            .setCoder(coder);
                    collections.put(name, FCollection.of(name, output, DataType.AVRO, outputAvroSchema));
                    break;
                }
                case STRUCT: {
                    final Type outputSchema = StructSchemaUtil.addStructField(
                            input.getSpannerType(),
                            Arrays.asList(
                                    Type.StructField.of(contentFieldName, Type.string()),
                                    Type.StructField.of(pageFieldName, Type.int64()),
                                    Type.StructField.of(fileSizeFieldName, Type.int64()),
                                    Type.StructField.of(titleFieldName, Type.string()),
                                    Type.StructField.of(authorFieldName, Type.string()),
                                    Type.StructField.of(subjectFieldName, Type.string()),
                                    Type.StructField.of(keywordsFieldName, Type.string()),
                                    Type.StructField.of(creatorFieldName, Type.string()),
                                    Type.StructField.of(producerFieldName, Type.string()),
                                    Type.StructField.of(creationDateFieldName, Type.timestamp()),
                                    Type.StructField.of(modificationDateFieldName, Type.timestamp()),
                                    Type.StructField.of(trappedFieldName, Type.string())));
                    final PDFExtract transform = new PDFExtract<>(config,
                            (Struct struct, String field) -> {
                                if (struct.getType().getFieldIndex(field) < 0) {
                                    return null;
                                }
                                final Type fieldType = struct.getColumnType(field);
                                if(struct.isNull(field)) {
                                    return null;
                                }
                                switch (fieldType.getCode()) {
                                    case STRING: {
                                        final String value = struct.getString(field);
                                        if (value.startsWith("gs://")) {
                                            LOG.error("readBytes from: " + value);
                                            byte[] bytes = StorageUtil.readBytes(value);
                                            if(bytes == null) {
                                                LOG.error("Bytes is null: " + value);
                                                return null;
                                            }
                                            return bytes;
                                        } else if(value.startsWith("/gs/")) {
                                            final String path = value.replaceFirst("/gs/", "gs://");
                                            LOG.error("readBytes from: " + path);
                                            byte[] bytes =  StorageUtil.readBytes(path);
                                            if(bytes == null) {
                                                LOG.error("Bytes is null: " + path);
                                                return null;
                                            }
                                            return bytes;
                                        }
                                        throw new IllegalArgumentException("Must not be null");
                                    }
                                    case BYTES:
                                        return struct.getBytes(field).toByteArray();
                                    default:
                                        throw new IllegalArgumentException("pdfField must be String or Bytes. " + fieldType.toString());
                                }
                            },
                            (Struct struct, Type type, String prefix, PDFContent content) -> {
                                final String text;
                                if (content.getTexts() != null && content.getTexts().size() > 0) {
                                    text = String.join(" ", content.getTexts());
                                } else {
                                    text = null;
                                }
                                final PDFDocumentInformation info = content.getInformation();
                                return StructSchemaUtil.toBuilder(struct)
                                        .set(prefix + FIELD_NAME_CONTENT).to(text)
                                        .set(prefix + FIELD_NAME_PAGE).to(content.getPage())
                                        .set(prefix + FIELD_NAME_FILESIZE).to(content.getFileSize())
                                        .set(prefix + FIELD_NAME_TITLE).to(info.getTitle())
                                        .set(prefix + FIELD_NAME_AUTHOR).to(info.getAuthor())
                                        .set(prefix + FIELD_NAME_SUBJECT).to(info.getSubject())
                                        .set(prefix + FIELD_NAME_KEYWORDS).to(info.getKeywords())
                                        .set(prefix + FIELD_NAME_CREATOR).to(info.getCreator())
                                        .set(prefix + FIELD_NAME_PRODUCER).to(info.getProducer())
                                        .set(prefix + FIELD_NAME_CREATIONDATE).to(StructSchemaUtil.toCloudTimestamp(info.getCreationDate()))
                                        .set(prefix + FIELD_NAME_MODIFICATIONDATE).to(StructSchemaUtil.toCloudTimestamp(info.getModificationDate()))
                                        .set(prefix + FIELD_NAME_TRAPPED).to(info.getTrapped())
                                        .build();
                            },
                            outputSchema,
                            s -> s);
                    final PCollection<?> output = ((PCollection<?>) (input.getCollection())
                            .apply(config.getName(), transform));
                    collections.put(name, FCollection.of(name, output, DataType.STRUCT, outputSchema));
                    break;
                }
                case ENTITY: {
                    final PDFExtract transform = new PDFExtract<>(config,
                            (Entity entity, String field) -> {
                                final Value value = entity.getPropertiesMap().getOrDefault(field, null);
                                if (value == null
                                        || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)
                                        || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)) {
                                    return null;
                                }
                                switch (value.getValueTypeCase()) {
                                    case STRING_VALUE: {
                                        final String stringValue = value.getStringValue();
                                        if (stringValue.startsWith("gs://")) {
                                            LOG.error("readBytes from: " + value);
                                            byte[] bytes = StorageUtil.readBytes(stringValue);
                                            if(bytes == null) {
                                                LOG.error("Bytes is null: " + value);
                                                return null;
                                            }
                                            return bytes;
                                        } else if(stringValue.startsWith("/gs/")) {
                                            final String path = stringValue.replaceFirst("/gs/", "gs://");
                                            LOG.error("readBytes from: " + path);
                                            byte[] bytes =  StorageUtil.readBytes(path);
                                            if(bytes == null) {
                                                LOG.error("Bytes is null: " + path);
                                                return null;
                                            }
                                            return bytes;
                                        }
                                        throw new IllegalArgumentException("Must not be null");
                                    }
                                    case BLOB_VALUE:
                                        return value.getBlobValue().toByteArray();
                                    default:
                                        throw new IllegalArgumentException("pdfField must be String or Bytes. " + value.getValueTypeCase().name());
                                }
                            },
                            (Entity entity, Schema schema, String prefix, PDFContent content) -> {
                                final String text;
                                if (content.getTexts() != null && content.getTexts().size() > 0) {
                                    text = String.join(" ", content.getTexts());
                                } else {
                                    text = null;
                                }
                                final Value nullValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                                final PDFDocumentInformation info = content.getInformation();
                                return Entity.newBuilder(entity)
                                        .putProperties(prefix + FIELD_NAME_CONTENT, text == null ? nullValue : Value.newBuilder().setStringValue(text).setExcludeFromIndexes(true).build())
                                        .putProperties(prefix + FIELD_NAME_PAGE, content.getPage() == null ? nullValue : Value.newBuilder().setIntegerValue(content.getPage()).build())
                                        .putProperties(prefix + FIELD_NAME_FILESIZE, content.getFileSize() == null ? nullValue : Value.newBuilder().setIntegerValue(content.getFileSize()).build())
                                        .putProperties(prefix + FIELD_NAME_TITLE, info.getTitle() == null ? nullValue : Value.newBuilder().setStringValue(info.getTitle()).build())
                                        .putProperties(prefix + FIELD_NAME_AUTHOR, info.getAuthor() == null ? nullValue : Value.newBuilder().setStringValue(info.getAuthor()).build())
                                        .putProperties(prefix + FIELD_NAME_SUBJECT, info.getSubject() == null ? nullValue : Value.newBuilder().setStringValue(info.getSubject()).build())
                                        .putProperties(prefix + FIELD_NAME_KEYWORDS, info.getKeywords() == null ? nullValue : Value.newBuilder().setStringValue(info.getKeywords()).build())
                                        .putProperties(prefix + FIELD_NAME_CREATOR, info.getCreator() == null ? nullValue : Value.newBuilder().setStringValue(info.getCreator()).build())
                                        .putProperties(prefix + FIELD_NAME_PRODUCER, info.getProducer() == null ? nullValue : Value.newBuilder().setStringValue(info.getProducer()).build())
                                        .putProperties(prefix + FIELD_NAME_CREATIONDATE, info.getCreationDate() == null ? nullValue : Value.newBuilder().setTimestampValue(EntitySchemaUtil.toProtoTimestamp(info.getCreationDate())).build())
                                        .putProperties(prefix + FIELD_NAME_MODIFICATIONDATE, info.getModificationDate() == null ? nullValue : Value.newBuilder().setTimestampValue(EntitySchemaUtil.toProtoTimestamp(info.getModificationDate())).build())
                                        .putProperties(prefix + FIELD_NAME_TRAPPED, info.getTrapped() == null ? nullValue : Value.newBuilder().setStringValue(info.getTrapped()).build())
                                        .build();
                            },
                            outputAvroSchema.toString(),
                            AvroSchemaUtil::convertSchema);
                    final PCollection<?> output = ((PCollection<?>) (input.getCollection())
                            .apply(config.getName(), transform));
                    collections.put(name, FCollection.of(name, output, DataType.ENTITY, outputAvroSchema));
                    break;
                }
                case BIGTABLE:
                    break;
                case MUTATION:
                    break;
                case TEXT:
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }
        return collections;
    }

    private static void validate(final PDFExtractTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BeamSQL module parameter missing!");
        }
        if(parameters.getField() == null) {
            throw new IllegalArgumentException("PDFExtract module required field parameter!");
        }
    }

    private static void setDefaultParameters(final PDFExtractTransformParameters parameters) {
        if(parameters.getPrefix() == null) {
            parameters.setPrefix("");
        }
    }

    public static class PDFExtract<T,InputSchemaT,RuntimeSchemaT> extends PTransform<PCollection<T>, PCollection<T>> {

        private final PDFExtractTransformParameters parameters;
        private final ContentExtractor<T> contentExtractor;
        private final ContentSetter<T,RuntimeSchemaT> contentSetter;
        private final InputSchemaT schema;
        private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;


        private PDFExtract(final TransformConfig config,
                           final ContentExtractor<T> contentExtractor,
                           final ContentSetter<T,RuntimeSchemaT> contentSetter,
                           final InputSchemaT schema,
                           final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter) {

            this.parameters = new Gson().fromJson(config.getParameters(), PDFExtractTransformParameters.class);
            this.contentExtractor = contentExtractor;
            this.contentSetter = contentSetter;
            this.schema = schema;
            this.schemaConverter = schemaConverter;
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            return input.apply("ExtractText", ParDo.of(new PDFExtractDoFn<>(
                    parameters.getField(), parameters.getPrefix(),
                    contentExtractor, contentSetter,
                    schema, schemaConverter)));
        }

        private static class PDFExtractDoFn<T,InputSchemaT,RuntimeSchemaT> extends DoFn<T, T> {

            private final Logger log = LoggerFactory.getLogger(PDFExtractDoFn.class);

            private final String field;
            private final String prefix;
            private final ContentExtractor<T> contentExtractor;
            private final ContentSetter<T, RuntimeSchemaT> contentSetter;

            private final InputSchemaT schema;
            private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;

            private transient RuntimeSchemaT runtimeSchema;
            private transient PDFTextStripper stripper;

            PDFExtractDoFn(final String field, final String prefix,
                           final ContentExtractor<T> contentExtractor,
                           final ContentSetter<T, RuntimeSchemaT> contentSetter,
                           final InputSchemaT schema,
                           final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter) {

                this.field = field;
                this.prefix = prefix;
                this.contentExtractor = contentExtractor;
                this.contentSetter = contentSetter;
                this.schema = schema;
                this.schemaConverter = schemaConverter;
            }

            @Setup
            public void setup() throws IOException {
                this.stripper = new PDFTextStripper();
                this.runtimeSchema = schemaConverter.convert(schema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T input = c.element();
                final byte[] bytes = contentExtractor.getContent(input, field);
                final PDFContent pdfContent = extract(bytes);
                final T output = contentSetter.setContent(input, runtimeSchema, prefix, pdfContent);
                c.output(output);
            }

            private PDFContent extract(final byte[] bytes) {
                if(bytes == null) {
                    LOG.warn("Content is null!");
                    return PDFContent.of();
                }
                try(final PDDocument document = PDDocument.load(bytes)) {
                    final List<String> textContents = new ArrayList<>();
                    int pageCount = document.getPages().getCount();
                    for (int page = 0; page < pageCount; page++) {
                        stripper.setStartPage(page);
                        stripper.setEndPage(page);
                        stripper.setSortByPosition(true);
                        stripper.setLineSeparator("");
                        stripper.setAddMoreFormatting(true);
                        stripper.setSuppressDuplicateOverlappingText(true);
                        stripper.setShouldSeparateByBeads(true);
                        try {
                            final String text = stripper.getText(document);
                            textContents.add(text);
                        } catch (Exception e) {
                            log.error("error: " + e.getMessage());
                            textContents.add(" ");
                        }
                    }
                    final PDFDocumentInformation information = PDFDocumentInformation.of(document.getDocumentInformation());
                    return PDFContent.of(textContents, document.getNumberOfPages(), bytes.length, information);
                } catch (IOException e) {
                    log.error("miss: " + e.getMessage());
                    return PDFContent.of();
                }
            }

        }

        private interface ContentExtractor<T> extends Serializable {
            byte[] getContent(final T input, final String field);
        }

        private interface ContentSetter<T, SchemaT> extends Serializable {
            T setContent(final T input, final SchemaT schema, final String prefix, final PDFContent content);
        }

        private interface SchemaConverter<InputSchemaT, OutputSchemaT> extends Serializable {
            OutputSchemaT convert(InputSchemaT schema);
        }

    }

    private static class PDFContent {

        private List<String> texts;
        private Integer page;
        private Integer fileSize;
        private PDFDocumentInformation information;

        public Integer getPage() {
            return page;
        }

        public void setPage(Integer page) {
            this.page = page;
        }

        public Integer getFileSize() {
            return fileSize;
        }

        public void setFileSize(Integer fileSize) {
            this.fileSize = fileSize;
        }

        public List<String> getTexts() {
            return texts;
        }

        public void setTexts(List<String> texts) {
            this.texts = texts;
        }

        public PDFDocumentInformation getInformation() {
            return information;
        }

        public void setInformation(PDFDocumentInformation information) {
            this.information = information;
        }

        public static PDFContent of() {
            final PDFContent content = new PDFContent();
            content.setTexts(new ArrayList<>());
            content.setPage(0);
            content.setFileSize(0);
            content.setInformation(new PDFDocumentInformation());
            return content;
        }

        public static PDFContent of(final List<String> texts, final int page, final int fileSize, final PDFDocumentInformation information) {
            final PDFContent content = new PDFContent();
            content.setTexts(texts);
            content.setPage(page);
            content.setFileSize(fileSize);
            content.setInformation(information);
            return content;
        }
    }

    private static class PDFDocumentInformation {

        private String title;
        private String author;
        private String subject;
        private String keywords;
        private String creator;
        private String producer;
        private Instant creationDate;
        private Instant modificationDate;
        private String trapped;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getKeywords() {
            return keywords;
        }

        public void setKeywords(String keywords) {
            this.keywords = keywords;
        }

        public String getCreator() {
            return creator;
        }

        public void setCreator(String creator) {
            this.creator = creator;
        }

        public String getProducer() {
            return producer;
        }

        public void setProducer(String producer) {
            this.producer = producer;
        }

        public Instant getCreationDate() {
            return creationDate;
        }

        public void setCreationDate(Instant creationDate) {
            this.creationDate = creationDate;
        }

        public Instant getModificationDate() {
            return modificationDate;
        }

        public void setModificationDate(Instant modificationDate) {
            this.modificationDate = modificationDate;
        }

        public String getTrapped() {
            return trapped;
        }

        public void setTrapped(String trapped) {
            this.trapped = trapped;
        }

        public static PDFDocumentInformation of(final PDDocumentInformation pdf) {
            final PDFDocumentInformation information = new PDFDocumentInformation();
            information.setTitle(pdf.getTitle());
            information.setAuthor(pdf.getAuthor());
            information.setSubject(pdf.getSubject());
            information.setKeywords(pdf.getKeywords());
            information.setCreator(pdf.getCreator());
            information.setProducer(pdf.getProducer());
            if(pdf.getCreationDate() != null) {
                information.setCreationDate(Instant.ofEpochMilli(pdf.getCreationDate().toInstant().toEpochMilli()));
            }
            if(pdf.getModificationDate() != null) {
                information.setModificationDate(Instant.ofEpochMilli(pdf.getModificationDate().toInstant().toEpochMilli()));
            }
            information.setTrapped(pdf.getTrapped());
            return information;
        }

    }

}
