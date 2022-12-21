package com.mercari.solution.config;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.converter.RecordToRowConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;

import java.io.Serializable;
import java.util.*;


public class SourceConfig implements Serializable {

    public static final String OPTION_ORIGINAL_FIELD_NAME = "originalFieldName";

    // source module properties
    private String name;
    private String module;
    private Boolean microbatch;
    private InputSchema schema;
    private JsonObject parameters;
    private List<String> wait;
    private String timestampAttribute;
    private String timestampDefault;
    private List<AdditionalOutput> additionalOutputs;
    private Boolean skip;

    private String description;

    // template args
    private Map<String, Object> args;

    // getter, setter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public Boolean getMicrobatch() {
        return microbatch;
    }

    public void setMicrobatch(Boolean microbatch) {
        this.microbatch = microbatch;
    }

    public InputSchema getSchema() {
        return schema;
    }

    public void setSchema(InputSchema schema) {
        this.schema = schema;
    }

    public JsonObject getParameters() {
        return parameters;
    }

    public void setParameters(JsonObject parameters) {
        this.parameters = parameters;
    }

    public List<String> getWait() {
        return wait;
    }

    public void setWait(List<String> wait) {
        this.wait = wait;
    }

    public String getTimestampAttribute() {
        return timestampAttribute;
    }

    public void setTimestampAttribute(String timestampAttribute) {
        this.timestampAttribute = timestampAttribute;
    }

    public String getTimestampDefault() {
        if(timestampDefault == null) {
            return "1970-01-01T00:00:00Z";
        }
        return timestampDefault;
    }

    public void setTimestampDefault(String timestampDefault) {
        this.timestampDefault = timestampDefault;
    }

    public List<AdditionalOutput> getAdditionalOutputs() {
        return additionalOutputs;
    }

    public void setAdditionalOutputs(List<AdditionalOutput> additionalOutputs) {
        this.additionalOutputs = additionalOutputs;
    }

    public Boolean getSkip() {
        return skip;
    }

    public void setSkip(Boolean skip) {
        this.skip = skip;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    public void setArgs(Map<String, Object> args) {
        this.args = args;
    }

    public static Schema convertSchema(final InputSchema inputSchema) {
        if(inputSchema.getAvroSchema() != null && inputSchema.getAvroSchema().startsWith("gs://")) {
            final String schemaString = StorageUtil.readString(inputSchema.getAvroSchema());
            return RecordToRowConverter.convertSchema(AvroSchemaUtil.convertSchema(schemaString));
        } else if(inputSchema.getFields() != null && inputSchema.getFields().size() > 0) {
            return convertSchema(inputSchema.getFields());
        } else {
            throw new IllegalArgumentException("SourceConfig does not contain schema.");
        }
    }

    public static org.apache.avro.Schema convertAvroSchema(final InputSchema inputSchema) {
        if(inputSchema.getAvroSchema() != null && inputSchema.getAvroSchema().startsWith("gs://")) {
            final String schemaString = StorageUtil.readString(inputSchema.getAvroSchema());
            return AvroSchemaUtil.convertSchema(schemaString);
        } else if(inputSchema.getFields() != null && inputSchema.getFields().size() > 0) {
            return RowToRecordConverter.convertSchema(convertSchema(inputSchema.getFields()));
        } else {
            throw new IllegalArgumentException("SourceConfig does not contain schema.");
        }
    }

    private static Schema convertSchema(final List<InputSchemaField> fields) {
        if(fields == null || fields.size() == 0) {
            return null;
        }
        final Schema.Builder builder = Schema.builder();
        for(final InputSchemaField inputSchemaField : fields) {
            final List<Schema.Options> optionsList = new ArrayList<>();
            final String fieldName;
            if(inputSchemaField.getAlterName() == null) {
                fieldName = inputSchemaField.getName();
            } else {
                fieldName = inputSchemaField.getAlterName();
                optionsList.add(Schema.Options.builder()
                        .setOption(OPTION_ORIGINAL_FIELD_NAME, Schema.FieldType.STRING, inputSchemaField.getName())
                        .build());
            }
            if(inputSchemaField.getOptions() != null) {
                for(final Map.Entry<String, String> entry : inputSchemaField.getOptions().entrySet()) {
                    optionsList.add(Schema.Options.builder()
                            .setOption(entry.getKey(), Schema.FieldType.STRING, entry.getValue())
                            .build());
                }
            }
            if("json".equalsIgnoreCase(inputSchemaField.getType().trim())) {
                optionsList.add(Schema.Options.builder()
                        .setOption("sqlType", Schema.FieldType.STRING, "JSON")
                        .build());
            }

            final Schema.FieldType fieldType = convertFieldType(inputSchemaField);
            Schema.Field field = Schema.Field.of(fieldName, fieldType);
            for(final Schema.Options fieldOptions : optionsList) {
                field = field.withOptions(fieldOptions);
            }
            builder.addField(field);
        }
        return builder.build();
    }

    public static org.apache.avro.Schema convertAvroSchema(final List<InputSchemaField> fields) {
        final Schema schema = convertSchema(fields);
        if(schema == null) {
            return null;
        }
        return RowToRecordConverter.convertSchema(schema);
    }

    public static Map<String, Descriptors.Descriptor> convertProtobufDescriptors(final InputSchema inputSchema) {
        if(inputSchema.getProtobufDescriptor() != null && inputSchema.getProtobufDescriptor().startsWith("gs://")) {
            final byte[] bytes = StorageUtil.readBytes(inputSchema.getProtobufDescriptor());
            return ProtoSchemaUtil.getDescriptors(bytes);
        } else {
            throw new IllegalArgumentException("SourceConfig does not contain protobuf descriptor file.");
        }
    }

    private static Schema.FieldType convertFieldType(final InputSchemaField field) {
        return convertFieldType(field, field.getMode());
    }

    private static Schema.FieldType convertFieldType(final InputSchemaField field, final String mode) {
        if(mode != null && "repeated".equalsIgnoreCase(mode.trim())) {
            return Schema.FieldType.array(convertFieldType(field, "nullable"));
        }
        final boolean nullable;
        if(mode == null) {
            nullable = true;
        } else {
            nullable = "nullable".equalsIgnoreCase(mode.trim());
        }
        switch (field.getType().trim().toLowerCase()) {
            case "bytes":
                return Schema.FieldType.BYTES.withNullable(nullable);
            case "json":
            case "string":
                return Schema.FieldType.STRING.withNullable(nullable);
            case "byte":
                return Schema.FieldType.BYTE.withNullable(nullable);
            case "short":
            case "int16":
                return Schema.FieldType.INT16.withNullable(nullable);
            case "int":
            case "integer":
            case "int32":
                return Schema.FieldType.INT32.withNullable(nullable);
            case "long":
            case "int64":
                return Schema.FieldType.INT64.withNullable(nullable);
            case "float32":
            case "float":
                return Schema.FieldType.FLOAT.withNullable(nullable);
            case "float64":
            case "double":
                return Schema.FieldType.DOUBLE.withNullable(nullable);
            case "numeric":
            case "decimal":
                return Schema.FieldType.DECIMAL.withNullable(nullable);
            case "bool":
            case "boolean":
                return Schema.FieldType.BOOLEAN.withNullable(nullable);
            case "time":
                return nullable ? CalciteUtils.NULLABLE_TIME : CalciteUtils.TIME;
            case "date":
                return nullable ? CalciteUtils.NULLABLE_DATE : CalciteUtils.DATE;
            case "datetime":
                return Schema.FieldType.logicalType(SqlTypes.DATETIME).withNullable(nullable);
            case "timestamp":
                return Schema.FieldType.DATETIME.withNullable(nullable);
            case "row":
            case "struct":
            case "record":
                return Schema.FieldType.row(convertSchema(field.getFields())).withNullable(nullable);
            case "map": {
                final InputSchemaField keyField = field.getFields().stream()
                        .filter(f -> "key".equals(f.getName()))
                        .findAny().orElse(new InputSchemaField("key", "string", "required"));
                final InputSchemaField valueField = field.getFields().stream()
                        .filter(f -> "value".equals(f.getName()))
                        .findAny().orElseThrow(() -> new IllegalArgumentException("Map schema must contain value field."));
                return Schema.FieldType.map(convertFieldType(keyField), convertFieldType(valueField)).withNullable(nullable);
            }
            case "maprecord": {
                final InputSchemaField keyField = field.getFields().stream()
                        .filter(f -> "key".equals(f.getName()))
                        .findAny().orElse(new InputSchemaField("key", "string", "required"));
                final InputSchemaField valueField = field.getFields().stream()
                        .filter(f -> "value".equals(f.getName()))
                        .findAny().orElseThrow(() -> new IllegalArgumentException("Maprecord schema must contain value field."));
                final Schema mapSchema = convertSchema(Arrays.asList(keyField, valueField))
                        .withOptions(Schema.Options.builder()
                                .setOption("extension", Schema.FieldType.STRING, "maprecord")
                                .build());
                final Schema.FieldType mapField = Schema.FieldType.row(mapSchema).withNullable(nullable);
                return Schema.FieldType.array(mapField);
            }
            default:
                throw new IllegalArgumentException("Field[" + field.getName() + "] type " + field.getType() + " is not supported !");
        }
    }

    public static class InputSchema implements Serializable {

        private String avroSchema;
        private String protobufDescriptor;
        private List<InputSchemaField> fields;

        public String getAvroSchema() {
            return avroSchema;
        }

        public void setAvroSchema(String avroSchema) {
            this.avroSchema = avroSchema;
        }

        public String getProtobufDescriptor() {
            return protobufDescriptor;
        }

        public void setProtobufDescriptor(String protobufDescriptor) {
            this.protobufDescriptor = protobufDescriptor;
        }

        public List<InputSchemaField> getFields() {
            return fields;
        }

        public void setFields(List<InputSchemaField> fields) {
            this.fields = fields;
        }

    }

    public static class InputSchemaField implements Serializable {

        private String name;
        private String type;
        private String mode;
        private List<InputSchemaField> fields;
        private Map<String, String> options;
        private String alterName;

        InputSchemaField() {

        }

        InputSchemaField(String name, String type, String mode) {
            this.name = name;
            this.type = type;
            this.mode = mode;
            this.options = new HashMap<>();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public List<InputSchemaField> getFields() {
            return fields;
        }

        public void setFields(List<InputSchemaField> fields) {
            this.fields = fields;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        public String getAlterName() {
            return alterName;
        }

        public void setAlterName(String alterName) {
            this.alterName = alterName;
        }
    }

    public static class AdditionalOutput implements Serializable {

        private String name;
        private JsonElement conditions;
        private InputSchema schema;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public JsonElement getConditions() {
            return conditions;
        }

        public void setConditions(JsonElement conditions) {
            this.conditions = conditions;
        }

        public InputSchema getSchema() {
            return schema;
        }

        public void setSchema(InputSchema schema) {
            this.schema = schema;
        }

        public Output toOutput() {
            final Output output = new Output();
            output.setName(this.name);
            output.setConditions(this.conditions.toString());
            output.setSchema(this.schema);
            return output;
        }

    }

    public static class Output implements Serializable {

        private String name;
        private String conditions;
        private InputSchema schema;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getConditions() {
            return conditions;
        }

        public void setConditions(String conditions) {
            this.conditions = conditions;
        }

        public InputSchema getSchema() {
            return schema;
        }

        public void setSchema(InputSchema schema) {
            this.schema = schema;
        }

        @Override
        public String toString() {
            return String.format("name: %s, leaves: %s", this.name, this.conditions);
        }

    }

}

