package com.mercari.solution.module.transform;

import com.google.api.services.storage.Storage;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.domain.text.HtmlUtil;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class PDFExtractTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(PDFExtractTransform.class);

    private static final String FIELD_NAME_CONTENT = "Content";
    private static final String FIELD_NAME_FILESIZE = "FileByteSize";
    private static final String FIELD_NAME_PAGE = "Page";
    private static final String FIELD_NAME_VERSION = "Version";
    private static final String FIELD_NAME_ENCRYPTED = "Encrypted";
    private static final String FIELD_NAME_TITLE = "Title";
    private static final String FIELD_NAME_AUTHOR = "Author";
    private static final String FIELD_NAME_SUBJECT = "Subject";
    private static final String FIELD_NAME_KEYWORDS = "Keywords";
    private static final String FIELD_NAME_CREATOR = "Creator";
    private static final String FIELD_NAME_PRODUCER = "Producer";
    private static final String FIELD_NAME_CREATIONDATE = "CreationDate";
    private static final String FIELD_NAME_MODIFICATIONDATE = "ModificationDate";
    private static final String FIELD_NAME_TRAPPED = "Trapped";
    private static final String FIELD_NAME_FAILED = "Failed";
    private static final String FIELD_NAME_ERROR_PAGE = "ErrorPageCount";
    private static final String FIELD_NAME_ERROR_MESSAGE = "ErrorMessage";

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

        private void validate() {
            if(field == null) {
                throw new IllegalArgumentException("PDFExtract module required field parameter!");
            }
        }

        private void setDefaults() {
            if(this.prefix == null) {
                this.prefix = "";
            }
        }
    }

    public String getName() { return "pdfextract"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return PDFExtractTransform.transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final PDFExtractTransformParameters parameters = new Gson().fromJson(config.getParameters(), PDFExtractTransformParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("PDFExtract module: " + config.getName() + " parameter missing!");
        }
        parameters.validate();
        parameters.setDefaults();

        final String contentFieldName = parameters.getPrefix() + FIELD_NAME_CONTENT;
        final String fileSizeFieldName = parameters.getPrefix() + FIELD_NAME_FILESIZE;
        final String pageFieldName = parameters.getPrefix() + FIELD_NAME_PAGE;
        final String versionFieldName = parameters.getPrefix() + FIELD_NAME_VERSION;
        final String encryptedFieldName = parameters.getPrefix() + FIELD_NAME_ENCRYPTED;
        final String titleFieldName = parameters.getPrefix() + FIELD_NAME_TITLE;
        final String authorFieldName = parameters.getPrefix() + FIELD_NAME_AUTHOR;
        final String subjectFieldName = parameters.getPrefix() + FIELD_NAME_SUBJECT;
        final String keywordsFieldName = parameters.getPrefix() + FIELD_NAME_KEYWORDS;
        final String creatorFieldName = parameters.getPrefix() + FIELD_NAME_CREATOR;
        final String producerFieldName = parameters.getPrefix() + FIELD_NAME_PRODUCER;
        final String creationDateFieldName = parameters.getPrefix() + FIELD_NAME_CREATIONDATE;
        final String modificationDateFieldName = parameters.getPrefix() + FIELD_NAME_MODIFICATIONDATE;
        final String trappedFieldName = parameters.getPrefix() + FIELD_NAME_TRAPPED;
        final String failedFieldName = parameters.getPrefix() + FIELD_NAME_FAILED;
        final String errorPageFieldName = parameters.getPrefix() + FIELD_NAME_ERROR_PAGE;
        final String errorMessageName = parameters.getPrefix() + FIELD_NAME_ERROR_MESSAGE;

        final Map<String, FCollection<?>> collections = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final boolean isContentFieldString = org.apache.beam.sdk.schemas.Schema.FieldType.STRING.getTypeName()
                    .equals(input.getSchema().getField(parameters.getField()).getType().getTypeName());

            switch (input.getDataType()) {
                case ROW: {
                    final org.apache.beam.sdk.schemas.Schema outputRowSchema = RowSchemaUtil.toBuilder(input.getSchema())
                            .addField(contentFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(false))
                            .addField(fileSizeFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                            .addField(pageFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                            .addField(versionFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(encryptedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(true))
                            .addField(titleFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(authorFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(subjectFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(keywordsFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(creatorFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(producerFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(creationDateFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                            .addField(modificationDateFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                            .addField(trappedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(failedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(false))
                            .addField(errorPageFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(false))
                            .addField(errorMessageName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .build();
                    final PDFExtract<org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema, Row> transform = new PDFExtract<>(
                            parameters,
                            outputRowSchema,
                            s -> s,
                            RowSchemaUtil::getAsString,
                            RowSchemaUtil::getBytes,
                            RowSchemaUtil::merge,
                            (Instant timestamp) -> timestamp,
                            isContentFieldString);
                    final PCollection<Row> output = ((PCollection<Row>) (input.getCollection()))
                            .apply(config.getName(), transform)
                            .setCoder(RowCoder.of(outputRowSchema));
                    collections.put(name, FCollection.of(name, output, DataType.ROW, outputRowSchema));
                    break;
                }
                case AVRO: {
                    final Schema outputAvroSchema = AvroSchemaUtil.toBuilder(input.getAvroSchema())
                            .name(contentFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(fileSizeFieldName).type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                            .name(pageFieldName).type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                            .name(versionFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(encryptedFieldName).type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                            .name(titleFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(authorFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(subjectFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(keywordsFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(creatorFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(producerFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(creationDateFieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                            .name(modificationDateFieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                            .name(trappedFieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .name(failedFieldName).type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                            .name(errorPageFieldName).type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                            .name(errorMessageName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                            .endRecord();
                    final PDFExtract<String,Schema,GenericRecord> transform = new PDFExtract<>(
                            parameters,
                            outputAvroSchema.toString(),
                            AvroSchemaUtil::convertSchema,
                            AvroSchemaUtil::getAsString,
                            AvroSchemaUtil::getBytes,
                            AvroSchemaUtil::merge,
                            (Instant timestamp) -> timestamp.getMillis() * 1000L,
                            isContentFieldString);
                    final PCollection<GenericRecord> output = ((PCollection<GenericRecord>) (input.getCollection()))
                            .apply(config.getName(), transform)
                            .setCoder(AvroCoder.of(outputAvroSchema));
                    collections.put(name, FCollection.of(name, output, DataType.AVRO, outputAvroSchema));
                    break;
                }
                case STRUCT: {
                    final Type outputType = StructSchemaUtil.addStructField(
                            input.getSpannerType(),
                            Arrays.asList(
                                    Type.StructField.of(contentFieldName, Type.string()),
                                    Type.StructField.of(fileSizeFieldName, Type.int64()),
                                    Type.StructField.of(pageFieldName, Type.int64()),
                                    Type.StructField.of(versionFieldName, Type.float64()),
                                    Type.StructField.of(encryptedFieldName, Type.bool()),
                                    Type.StructField.of(titleFieldName, Type.string()),
                                    Type.StructField.of(authorFieldName, Type.string()),
                                    Type.StructField.of(subjectFieldName, Type.string()),
                                    Type.StructField.of(keywordsFieldName, Type.string()),
                                    Type.StructField.of(creatorFieldName, Type.string()),
                                    Type.StructField.of(producerFieldName, Type.string()),
                                    Type.StructField.of(creationDateFieldName, Type.timestamp()),
                                    Type.StructField.of(modificationDateFieldName, Type.timestamp()),
                                    Type.StructField.of(trappedFieldName, Type.string()),
                                    Type.StructField.of(failedFieldName, Type.bool()),
                                    Type.StructField.of(errorPageFieldName, Type.int64()),
                                    Type.StructField.of(errorMessageName, Type.string())));
                    final PDFExtract<Type, Type, Struct> transform = new PDFExtract<>(
                            parameters,
                            outputType,
                            s -> s,
                            StructSchemaUtil::getAsString,
                            StructSchemaUtil::getBytes,
                            StructSchemaUtil::merge,
                            (Instant timestamp) -> Timestamp.ofTimeMicroseconds(timestamp.getMillis() * 1000L),
                            isContentFieldString);
                    final PCollection<Struct> output = ((PCollection<Struct>) (input.getCollection()))
                            .apply(config.getName(), transform);
                    collections.put(name, FCollection.of(name, output, DataType.STRUCT, outputType));
                    break;
                }
                case DOCUMENT: {
                    final org.apache.beam.sdk.schemas.Schema outputRowSchema = RowSchemaUtil.toBuilder(input.getSchema())
                            .addField(contentFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(false))
                            .addField(fileSizeFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                            .addField(pageFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                            .addField(versionFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(encryptedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(true))
                            .addField(titleFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(authorFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(subjectFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(keywordsFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(creatorFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(producerFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(creationDateFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                            .addField(modificationDateFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                            .addField(trappedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(failedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(false))
                            .addField(errorPageFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(false))
                            .addField(errorMessageName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .build();
                    final PDFExtract<org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema, Document> transform = new PDFExtract<>(
                            parameters,
                            outputRowSchema,
                            s -> s,
                            DocumentSchemaUtil::getAsString,
                            DocumentSchemaUtil::getBytes,
                            DocumentSchemaUtil::merge,
                            (Instant timestamp) -> timestamp,
                            isContentFieldString);
                    final PCollection<Document> output = ((PCollection<Document>) (input.getCollection()))
                            .apply(config.getName(), transform);
                    collections.put(name, FCollection.of(name, output, DataType.DOCUMENT, outputRowSchema));
                    break;
                }
                case ENTITY: {
                    final org.apache.beam.sdk.schemas.Schema outputRowSchema = RowSchemaUtil.toBuilder(input.getSchema())
                            .addField(contentFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(false))
                            .addField(fileSizeFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                            .addField(pageFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                            .addField(versionFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(encryptedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(true))
                            .addField(titleFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(authorFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(subjectFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(keywordsFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(creatorFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(producerFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(creationDateFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                            .addField(modificationDateFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                            .addField(trappedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .addField(failedFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(false))
                            .addField(errorPageFieldName, org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(false))
                            .addField(errorMessageName, org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                            .build();
                    final PDFExtract<org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema, Entity> transform = new PDFExtract<>(
                            parameters,
                            outputRowSchema,
                            s -> s,
                            EntitySchemaUtil::getAsString,
                            EntitySchemaUtil::getBytes,
                            EntitySchemaUtil::merge,
                            (Instant timestamp) -> DateTimeUtil.toProtoTimestamp(timestamp.getMillis() * 1000L),
                            isContentFieldString);
                    final PCollection<Entity> output = ((PCollection<Entity>) (input.getCollection()))
                            .apply(config.getName(), transform);
                    collections.put(name, FCollection.of(name, output, DataType.ENTITY, outputRowSchema));
                    break;
                }
                default:
                    throw new IllegalArgumentException("PDFExtract: "+ config.getName()+ " Not supported input type: " + input.getDataType());
            }
        }
        return collections;
    }

    public static class PDFExtract<InputSchemaT, RuntimeSchemaT, T> extends PTransform<PCollection<T>, PCollection<T>> {

        private final PDFExtractTransformParameters parameters;
        private final InputSchemaT schema;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.StringGetter<T> stringGetter;
        private final SchemaUtil.BytesGetter<T> bytesGetter;
        private final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter;
        private final TimestampConverter timestampConverter;
        private final boolean isContentFieldString;


        private PDFExtract(final PDFExtractTransformParameters parameters,
                           final InputSchemaT schema,
                           final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                           final SchemaUtil.StringGetter<T> stringGetter,
                           final SchemaUtil.BytesGetter<T> bytesGetter,
                           final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter,
                           final TimestampConverter timestampConverter,
                           final boolean isContentFieldString) {

            this.parameters = parameters;
            this.schema = schema;
            this.schemaConverter = schemaConverter;
            this.stringGetter =  stringGetter;
            this.bytesGetter = bytesGetter;
            this.valuesSetter = valuesSetter;
            this.timestampConverter = timestampConverter;
            this.isContentFieldString = isContentFieldString;
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            final PDFExtractDoFn<T,InputSchemaT,RuntimeSchemaT> dofn = new PDFExtractDoFn<>(
                    parameters.getField(), parameters.getPrefix(),
                    schema, schemaConverter,
                    stringGetter, bytesGetter, valuesSetter, timestampConverter,
                    isContentFieldString);

            if(OptionUtil.isStreaming(input)) {
                return input.apply("Extract", ParDo.of(dofn));
            } else {
                return input
                        .apply("Reshuffle", Reshuffle.viaRandomKey())
                        .apply("Extract", ParDo.of(dofn));
            }
        }

        private static class PDFExtractDoFn<T,InputSchemaT,RuntimeSchemaT> extends DoFn<T, T> {

            private final String field;
            private final String prefix;
            private final boolean isContentFieldString;

            private final InputSchemaT schema;
            private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.StringGetter<T> stringGetter;
            private final SchemaUtil.BytesGetter<T> bytesGetter;
            private final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter;
            private final TimestampConverter timestampConverter;

            private transient RuntimeSchemaT runtimeSchema;
            private transient PDFTextStripper stripper;
            private transient Storage storage;

            PDFExtractDoFn(final String field,
                           final String prefix,
                           final InputSchemaT schema,
                           final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                           final SchemaUtil.StringGetter<T> stringGetter,
                           final SchemaUtil.BytesGetter<T> bytesGetter,
                           final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter,
                           final TimestampConverter  timestampConverter,
                           final boolean isContentFieldString) {

                this.field = field;
                this.prefix = prefix;
                this.schema = schema;
                this.schemaConverter = schemaConverter;
                this.stringGetter = stringGetter;
                this.bytesGetter = bytesGetter;
                this.valuesSetter = valuesSetter;
                this.timestampConverter = timestampConverter;
                this.isContentFieldString = isContentFieldString;
            }

            @Setup
            public void setup() throws IOException {
                this.stripper = new PDFTextStripper();
                this.runtimeSchema = schemaConverter.convert(schema);
                if(isContentFieldString) {
                    this.storage = StorageUtil.storage();
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T input = c.element();
                final byte[] bytes;
                String errorMessage = null;
                if(isContentFieldString) {
                    String stringFieldValue = stringGetter.getAsString(input, field);
                    if(stringFieldValue == null) {
                        errorMessage = "pdf content field: " + field + " value is null";
                        LOG.warn(errorMessage);
                        bytes = null;
                    } else {
                        if(stringFieldValue.startsWith("/gs/")) { // modify app engine style gcs path
                            stringFieldValue = stringFieldValue.replaceFirst("/gs/", "gs://");
                        }
                        if(stringFieldValue.startsWith("gs://")) {
                            LOG.info("Read pdf content from gcs path: " + stringFieldValue);
                            bytes = StorageUtil.readBytes(storage, stringFieldValue);
                        } else if(stringFieldValue.startsWith("https://") || stringFieldValue.startsWith("http://")) {
                            LOG.info("Read pdf content from url: " + stringFieldValue);
                            bytes = null;
                        } else {
                            errorMessage = "Not supported pdf content uri: " + stringFieldValue;
                            LOG.warn(errorMessage);
                            bytes = null;
                        }
                    }
                } else {
                    bytes = bytesGetter.getAsBytes(input, field);
                    if(bytes == null) {
                        errorMessage = "PDF content field: " + field + " value is null";
                    }
                }

                final Map<String, Object> pdfContent = extractPDF(bytes);
                if(errorMessage != null) {
                    pdfContent.put(prefix + FIELD_NAME_ERROR_MESSAGE, errorMessage);
                }
                final T output = valuesSetter.setValues(runtimeSchema, input, pdfContent);
                c.output(output);
            }

            private Map<String, Object> extractPDF(final byte[] bytes) {
                if(bytes == null) {
                    return createEmpty(bytes, "content is null");
                }

                final Map<String, Object> values = new HashMap<>();
                final List<String> pageErrorMessages = new ArrayList<>();
                try(final PDDocument document = PDDocument.load(bytes)) {
                    final List<String> textContents = new ArrayList<>();
                    int pageCount = document.getPages().getCount();
                    long errorPageCount = 0;
                    for (int page = 0; page <= pageCount; page++) {
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
                            final String errorMessage = "page: " + page + ", error: " + e.getMessage();
                            pageErrorMessages.add(errorMessage);
                            LOG.error(errorMessage);
                            textContents.add(" ");
                            errorPageCount += 1;
                        }
                    }

                    final String content = String.join("", textContents);

                    values.put(prefix + FIELD_NAME_CONTENT, content);
                    values.put(prefix + FIELD_NAME_FILESIZE, Integer.valueOf(bytes.length).longValue());
                    values.put(prefix + FIELD_NAME_PAGE, Integer.valueOf(document.getNumberOfPages()).longValue());
                    values.put(prefix + FIELD_NAME_VERSION, Float.valueOf(document.getVersion()).toString());
                    values.put(prefix + FIELD_NAME_ENCRYPTED, document.isEncrypted());
                    values.put(prefix + FIELD_NAME_TITLE, document.getDocumentInformation().getTitle());
                    values.put(prefix + FIELD_NAME_AUTHOR, document.getDocumentInformation().getAuthor());
                    values.put(prefix + FIELD_NAME_SUBJECT, document.getDocumentInformation().getSubject());
                    values.put(prefix + FIELD_NAME_KEYWORDS, document.getDocumentInformation().getKeywords());
                    values.put(prefix + FIELD_NAME_CREATOR, document.getDocumentInformation().getCreator());
                    values.put(prefix + FIELD_NAME_PRODUCER, document.getDocumentInformation().getProducer());

                    final Calendar creationDate = document.getDocumentInformation().getCreationDate();
                    if(creationDate != null) {
                        values.put(prefix + FIELD_NAME_CREATIONDATE, timestampConverter.toTimestamp(Instant.ofEpochMilli(creationDate.toInstant().toEpochMilli())));
                    } else  {
                        values.put(prefix + FIELD_NAME_CREATIONDATE, null);
                    }

                    final Calendar modificationDate = document.getDocumentInformation().getModificationDate();
                    if(modificationDate != null) {
                        values.put(prefix + FIELD_NAME_MODIFICATIONDATE, timestampConverter.toTimestamp(Instant.ofEpochMilli(modificationDate.toInstant().toEpochMilli())));
                    } else  {
                        values.put(prefix + FIELD_NAME_MODIFICATIONDATE, null);
                    }

                    values.put(prefix + FIELD_NAME_TRAPPED, document.getDocumentInformation().getTrapped());
                    values.put(prefix + FIELD_NAME_FAILED, false);
                    values.put(prefix + FIELD_NAME_ERROR_PAGE, errorPageCount);

                    if(pageErrorMessages.size() > 0) {
                        values.put(prefix + FIELD_NAME_ERROR_MESSAGE, String.join(", ", pageErrorMessages));
                    }

                    return values;
                } catch (final Exception e) {
                    if(HtmlUtil.isZip(bytes)) {
                        try {
                            HtmlUtil.EPUBDocument document = HtmlUtil.readEPUB(bytes);
                            values.put(prefix + FIELD_NAME_CONTENT, document.getContent());
                            values.put(prefix + FIELD_NAME_PAGE, document.getPage());
                            values.put(prefix + FIELD_NAME_FILESIZE, Integer.valueOf(bytes.length).longValue());
                            values.put(prefix + FIELD_NAME_FAILED, false);
                            values.put(prefix + FIELD_NAME_ERROR_PAGE, 0L);
                            values.put(prefix + FIELD_NAME_ERROR_MESSAGE, e.getMessage());
                            return values;
                        } catch (Exception ee) {
                            LOG.error("Failed to parse epub cause: " + ee.getMessage());
                            return createEmpty(bytes, ee.getMessage());
                        }
                    } else {
                        LOG.error("Failed to parse pdf cause: " + e.getMessage());
                        return createEmpty(bytes, e.getMessage());
                    }
                }
            }

            private Map<String, Object> createEmpty(byte[] bytes, String message) {
                final Map<String, Object> values = new HashMap<>();
                values.put(prefix + FIELD_NAME_CONTENT, "");
                values.put(prefix + FIELD_NAME_PAGE, 0L);
                values.put(prefix + FIELD_NAME_FILESIZE, bytes == null ? 0L : Integer.valueOf(bytes.length).longValue());
                values.put(prefix + FIELD_NAME_FAILED, true);
                values.put(prefix + FIELD_NAME_ERROR_PAGE, 0L);
                values.put(prefix + FIELD_NAME_ERROR_MESSAGE, message);
                return values;
            }

        }

        private interface TimestampConverter extends Serializable {
            Object toTimestamp(final Instant timestamp);
        }

    }

}
