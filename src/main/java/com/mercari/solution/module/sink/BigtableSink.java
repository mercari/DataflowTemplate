package com.mercari.solution.module.sink;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.collect.Lists;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
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
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


public class BigtableSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableSink.class);

    private class BigtableSinkParameters {

        private String projectId;
        private String instanceId;
        private String tableId;

        private List<String> rowKeyFields;
        private String columnFamily;
        private String columnQualifier;

        private String rowKeyTemplate;
        private String columnFamilyTemplate;
        private String columnQualifierTemplate;

        private Format format;
        private List<ColumnSetting> columnSettings;
        private String separator;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getTableId() {
            return tableId;
        }

        public void setTableId(String tableId) {
            this.tableId = tableId;
        }

        public List<String> getRowKeyFields() {
            return rowKeyFields;
        }

        public void setRowKeyFields(List<String> rowKeyFields) {
            this.rowKeyFields = rowKeyFields;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public void setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
        }

        public String getColumnQualifier() {
            return columnQualifier;
        }

        public void setColumnQualifier(String columnQualifier) {
            this.columnQualifier = columnQualifier;
        }

        public String getRowKeyTemplate() {
            return rowKeyTemplate;
        }

        public void setRowKeyTemplate(String rowKeyTemplate) {
            this.rowKeyTemplate = rowKeyTemplate;
        }

        public String getColumnFamilyTemplate() {
            return columnFamilyTemplate;
        }

        public void setColumnFamilyTemplate(String columnFamilyTemplate) {
            this.columnFamilyTemplate = columnFamilyTemplate;
        }

        public String getColumnQualifierTemplate() {
            return columnQualifierTemplate;
        }

        public void setColumnQualifierTemplate(String columnQualifierTemplate) {
            this.columnQualifierTemplate = columnQualifierTemplate;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }

        public String getSeparator() {
            return separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }

        public List<ColumnSetting> getColumnSettings() {
            return columnSettings;
        }

        public void setColumnSettings(List<ColumnSetting> columnSettings) {
            this.columnSettings = columnSettings;
        }

        public void setDefaults() {
            if(format == null) {
                format = Format.string;
            }
            if(columnQualifier == null) {
                columnQualifier = "body";
            }
            if(separator == null) {
                separator = "#";
            }
            if(columnSettings == null) {
                columnSettings = new ArrayList<>();
            } else {
                for(var setting : columnSettings) {
                    setting.setDefaults(format, columnFamily);
                }
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(projectId == null) {
                errorMessages.add("BigtableSink module requires `projectId` parameter.");
            }
            if(instanceId == null) {
                errorMessages.add("BigtableSink module requires `instanceId` parameter.");
            }
            if(tableId == null) {
                errorMessages.add("BigtableSink module requires `tableId` parameter.");
            }
            if((rowKeyFields == null || rowKeyFields.size() == 0) && rowKeyTemplate == null) {
                errorMessages.add("BigtableSink module requires `rowKeyFields` or `rowKeyTemplate` parameter.");
            }
            if(columnFamily == null && columnFamilyTemplate == null) {
                errorMessages.add("BigtableSink module requires `columnFamily` or `columnFamilyTemplate` parameter.");
            }
            if(columnSettings != null) {
                for(var setting : columnSettings) {
                    setting.validate();
                }
            }
            return errorMessages;
        }

    }

    public enum Format implements Serializable {
        bytes,
        string,
        avro
    }

    public class ColumnSetting implements Serializable {

        private String field;
        private String columnFamily;
        private String columnQualifier;
        private Boolean exclude;
        private Format format;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public void setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
        }

        public String getColumnQualifier() {
            return columnQualifier;
        }

        public void setColumnQualifier(String columnQualifier) {
            this.columnQualifier = columnQualifier;
        }

        public Boolean getExclude() {
            return exclude;
        }

        public void setExclude(Boolean exclude) {
            this.exclude = exclude;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }


        public void setDefaults(final Format format, final String defaultColumnFamily) {
            if (columnQualifier == null) {
                columnQualifier = field;
            }
            if (columnFamily == null) {
                columnFamily = defaultColumnFamily;
            }
            if (exclude == null) {
                exclude = false;
            }
            if (this.format == null) {
                this.format = format;
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (field == null) {
                errorMessages.add("BigtableSink module's mappings parameter requires `field` parameter.");
            }
            return errorMessages;
        }
    }



    public String getName() { return "bigtable"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), BigtableSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> input, final SinkConfig config) {
        return write(input, config, null);
    }

    public static FCollection<?> write(final FCollection<?> input, final SinkConfig config, final List<FCollection<?>> waits) {
        final BigtableSinkParameters parameters = new Gson().fromJson(config.getParameters(), BigtableSinkParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        try {
            config.outputAvroSchema(input.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

        final PCollection output;
        switch (input.getDataType()) {
            case AVRO: {
                final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                final Write<GenericRecord, String, Schema> write = new Write<>(
                        parameters, input.getAvroSchema().toString(),
                        AvroSchemaUtil::convertSchema, AvroSchemaUtil::getAsString, RecordToMapConverter::convert,
                        RecordToBigtableConverter::convert, (s, r) -> r, AvroSchemaUtil::convertSchema);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            case ROW: {
                final FCollection<Row> inputCollection = (FCollection<Row>) input;
                final Write<Row, org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema> write = new Write<>(
                        parameters, input.getSchema(),
                        s -> s, RowSchemaUtil::getAsString, RowToMapConverter::convert,
                        RowToBigtableConverter::convert, RowToRecordConverter::convert, RowToRecordConverter::convertSchema);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            case STRUCT: {
                final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                final Write<Struct, Type, Type> write = new Write<>(
                        parameters, input.getSpannerType(),
                        t -> t, StructSchemaUtil::getAsString, StructToMapConverter::convert,
                        StructToBigtableConverter::convert, StructToRecordConverter::convert, StructToRecordConverter::convertSchema);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            case ENTITY: {
                final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                final Write<Entity, org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema> write = new Write<>(
                        parameters, input.getSchema(),
                        s -> s, EntitySchemaUtil::getAsString, EntityToMapConverter::convert,
                        EntityToBigtableConverter::convert, EntityToRecordConverter::convert, RowToRecordConverter::convertSchema);
                output = inputCollection.getCollection().apply(config.getName(), write);
                break;
            }
            default:
                throw new IllegalStateException();
        }
        return FCollection.update(input, output);
    }

    private static void validateParameters(final BigtableSinkParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BigtableSink parameters must not be empty!");
        }

        final List<String> errorMessages = parameters.validate();
        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
        }
    }

    private static void setDefaultParameters(final BigtableSinkParameters parameters) {
        parameters.setDefaults();
    }

    public static class Write<T,InputSchemaT,RuntimeSchemaT> extends PTransform<PCollection<T>, PCollection<BigtableWriteResult>> {

        private static final Logger LOG = LoggerFactory.getLogger(Write.class);

        private final InputSchemaT inputSchema;
        private final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final StringGetter<T> stringGetter;
        private final MapConverter<T> mapConverter;
        private final MutationConverter<T,RuntimeSchemaT> mutationConverter;
        private final AvroConverter<T> avroConverter;
        private final AvroSchemaConverter<InputSchemaT> avroSchemaConverter;

        private final BigtableSinkParameters parameters;

        private Write(final BigtableSinkParameters parameters,
                      final InputSchemaT inputSchema,
                      final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                      final StringGetter<T> stringGetter,
                      final MapConverter<T> mapConverter,
                      final MutationConverter<T,RuntimeSchemaT> mutationConverter,
                      final AvroConverter<T> avroConverter,
                      final AvroSchemaConverter<InputSchemaT> avroSchemaConverter) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.stringGetter = stringGetter;
            this.mapConverter = mapConverter;
            this.mutationConverter = mutationConverter;
            this.avroConverter = avroConverter;
            this.avroSchemaConverter = avroSchemaConverter;
        }

        public PCollection<BigtableWriteResult> expand(final PCollection<T> input) {

            final PCollection<BigtableWriteResult> writeResults = input
                    .apply("ToMutation", ParDo.of(new MutationDoFn<>(
                            parameters.getRowKeyFields(), parameters.getColumnFamily(), parameters.getColumnQualifier(),
                            parameters.getRowKeyTemplate(), parameters.getColumnFamilyTemplate(), parameters.getColumnQualifierTemplate(),
                            parameters.getFormat(), parameters.getColumnSettings(), parameters.getSeparator(),
                            inputSchema, schemaConverter, stringGetter, mapConverter, mutationConverter, avroConverter, avroSchemaConverter)))
                    .apply("WriteBigtable", BigtableIO.write()
                            .withProjectId(parameters.getProjectId())
                            .withInstanceId(parameters.getInstanceId())
                            .withTableId(parameters.getTableId())
                            .withoutValidation()
                            .withWriteResults());

            return writeResults;
        }

    }

    private static class MutationDoFn<T,InputSchemaT,RuntimeSchemaT> extends DoFn<T, KV<ByteString, Iterable<Mutation>>> {

        private static final DateTimeUtil.DateTimeTemplateUtils datetimeUtils = new DateTimeUtil.DateTimeTemplateUtils();

        private final List<String> rowKeyFields;
        private final String columnFamily;
        private final String columnQualifier;

        private final String rowKeyTemplate;
        private final String columnFamilyTemplate;
        private final String columnQualifierTemplate;

        private final Format format;
        private final String separator;
        private final Map<String, ColumnSetting> columnSettings;

        private final InputSchemaT inputSchema;
        private final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final StringGetter<T> stringGetter;
        private final MapConverter<T> mapConverter;
        private final MutationConverter<T,RuntimeSchemaT> mutationConverter;
        private final AvroConverter<T> avroConverter;
        private final AvroSchemaConverter<InputSchemaT> avroSchemaConverter;

        private transient RuntimeSchemaT runtimeSchema;
        private transient Schema avroSchema;

        private transient Template templateRowKey;
        private transient Template templateColumnFamily;
        private transient Template templateColumnQualifier;


        public MutationDoFn(final List<String> rowKeyFields,
                            final String columnFamily,
                            final String columnQualifier,
                            final String rowKeyTemplate,
                            final String columnFamilyTemplate,
                            final String columnQualifierTemplate,
                            final Format format,
                            final List<ColumnSetting> columnSettings,
                            final String separator,
                            final InputSchemaT inputSchema,
                            final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                            final StringGetter<T> stringGetter,
                            final MapConverter<T> mapConverter,
                            final MutationConverter<T,RuntimeSchemaT> mutationConverter,
                            final AvroConverter<T> avroConverter,
                            final AvroSchemaConverter<InputSchemaT> avroSchemaConverter) {

            this.rowKeyFields = rowKeyFields;
            this.columnFamily = columnFamily;
            this.columnQualifier = columnQualifier;
            this.rowKeyTemplate = rowKeyTemplate;
            this.columnFamilyTemplate = columnFamilyTemplate;
            this.columnQualifierTemplate = columnQualifierTemplate;

            this.format = format;
            this.columnSettings = columnSettings.stream().collect(Collectors.toMap(ColumnSetting::getField, c -> c));
            this.separator = separator;

            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.stringGetter = stringGetter;
            this.mapConverter = mapConverter;
            this.mutationConverter = mutationConverter;
            this.avroConverter = avroConverter;
            this.avroSchemaConverter = avroSchemaConverter;
        }

        @Setup
        public void setup() {
            this.runtimeSchema = schemaConverter.convert(inputSchema);
            if(rowKeyTemplate != null) {
                this.templateRowKey = TemplateUtil.createStrictTemplate("rowKeyTemplate", rowKeyTemplate);
            } else {
                this.templateRowKey = null;
            }
            if(columnFamilyTemplate != null) {
                this.templateColumnFamily = TemplateUtil.createStrictTemplate("columnFamilyTemplate", columnFamilyTemplate);
            } else {
                this.templateColumnFamily = null;
            }
            if(columnQualifierTemplate != null) {
                this.templateColumnQualifier = TemplateUtil.createStrictTemplate("columnQualifierTemplate", columnQualifierTemplate);;
            } else {
                this.templateColumnQualifier = null;
            }
            this.avroSchema = avroSchemaConverter.convert(inputSchema);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws IOException {

            // Generate template data
            final T element = c.element();
            final Map<String,Object> data;
            if(templateRowKey == null && templateColumnFamily == null && templateColumnQualifier == null) {
                data = null;
            } else {
                data = mapConverter.convert(element);
                data.put("_DateTimeUtil", datetimeUtils);
                data.put("_EVENTTIME", Instant.ofEpochMilli(c.timestamp().getMillis()));
            }

            // Generate columnFamily
            final String cf;
            if(templateColumnFamily == null) {
                cf = columnFamily;
            } else {
                cf = TemplateUtil.executeStrictTemplate(templateColumnFamily, data);
            }

            // Generate columnQualifier
            final String cq;
            if(templateColumnQualifier == null) {
                cq = columnQualifier;
            } else {
                cq = TemplateUtil.executeStrictTemplate(templateColumnQualifier, data);
            }

            // Generate mutations
            final Iterable<Mutation> mutations;
            switch (format) {
                case bytes:
                case string: {
                    mutations = mutationConverter.convert(runtimeSchema, element, cf, format, columnSettings);
                    break;
                }
                case avro: {
                    final GenericRecord record = avroConverter.convert(avroSchema, element);
                    final byte[] bytes = AvroSchemaUtil.encode(record);
                    final Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                            .setFamilyName(cf)
                            .setColumnQualifier(ByteString.copyFrom(cq, StandardCharsets.UTF_8))
                            .setValue(ByteString.copyFrom(bytes))
                            .build();
                    final Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
                    mutations = Lists.newArrayList(mutation);
                    break;
                }
                default: {
                    throw new IllegalStateException("BigtableSink not supported format: " + format);
                }
            }

            // Generate rowKey
            final String rowKeyString;
            if(rowKeyFields != null && rowKeyFields.size() > 0) {
                final StringBuilder sb = new StringBuilder();
                for(final String field : rowKeyFields) {
                    final String value = stringGetter.getAsString(element, field);
                    sb.append(value);
                    sb.append(separator);
                }
                if(sb.length() > 0) {
                    sb.deleteCharAt(sb.length() - separator.length());
                }
                rowKeyString = sb.toString();
            } else if(rowKeyTemplate != null) {
                rowKeyString = TemplateUtil.executeStrictTemplate(templateRowKey, data);
            } else {
                throw new IllegalStateException("Both rowKeyFields and rowKeyTemplate are null!");
            }
            final ByteString rowKey = ByteString.copyFrom(rowKeyString, StandardCharsets.UTF_8);

            final KV<ByteString, Iterable<Mutation>> output = KV.of(rowKey, mutations);
            c.output(output);
        }

    }


    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(InputSchemaT schema);
    }

    private interface MapConverter<T> extends Serializable {
        Map<String, Object> convert(T element);
    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(T element, String field);
    }

    private interface MutationConverter<T, SchemaT> extends Serializable {
        Iterable<Mutation> convert(final SchemaT schema, final T element,
                                   final String defaultColumnFamily,
                                   final Format defaultFormat,
                                   final Map<String,ColumnSetting> columnSettings);
    }

    private interface AvroConverter<T> extends Serializable {
        GenericRecord convert(final Schema schema, final T element);
    }

    private interface AvroSchemaConverter<InputSchemaT> extends Serializable {
        Schema convert(InputSchemaT schema);
    }

}
