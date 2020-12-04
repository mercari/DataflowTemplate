package com.mercari.solution.util.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TableRowToRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TableRowToRecordConverter.class);

    public static Schema convertSchema(final TableSchema tableSchema) {
        return convertSchema(tableSchema.getFields());
    }

    public static GenericRecord convert(final SchemaAndRecord schemaAndRecord) {
        //schemaAndRecord.g
        return null;
    }

    private static Schema convertSchema(final List<TableFieldSchema> fields) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        fields.forEach(field -> schemaFields
                .name(field.getName())
                .type(convertFieldType(field, false))
                .noDefault());
        return schemaFields.endRecord();
    }

    private static Schema convertFieldType(final TableFieldSchema schema, final boolean inarray) {
        final boolean nullable = !"REQUIRED".equals(schema.getMode());
        if(!inarray && "REPEATED".equals(schema.getMode())) {
            final Schema arraySchema = Schema.createArray(convertFieldType(schema, true));
            return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), arraySchema) : arraySchema;
        }
        switch (schema.getType()) {
            case "BYTES":
                return nullable ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
            case "STRING":
                return nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
            case "INT64":
            case "INTEGER":
                if(schema.containsKey("avroSchema") && "INT".equals(schema.get("avroSchema"))) {
                    return nullable ? AvroSchemaUtil.NULLABLE_INT : AvroSchemaUtil.REQUIRED_INT;
                }
                return nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
            case "FLOAT64":
            case "FLOAT":
                if(schema.containsKey("avroSchema") && "FLOAT".equals(schema.get("avroSchema"))) {
                    return nullable ? AvroSchemaUtil.NULLABLE_FLOAT : AvroSchemaUtil.NULLABLE_FLOAT;
                }
                return nullable ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
            case "BOOLEAN":
                return nullable ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
            case "DATE":
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
            case "TIME":
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MILLI_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MILLI_TYPE;
            case "DATETIME":
                return nullable ? AvroSchemaUtil.NULLABLE_SQL_DATETIME_TYPE : AvroSchemaUtil.REQUIRED_SQL_DATETIME_TYPE;
            case "TIMESTAMP":
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case "NUMERIC": {
                int precision = schema.get("precision") == null ? 38 : (int)schema.get("precision");
                int scale = schema.get("scale") == null ? 9 : (int)schema.get("scale");
                final Schema numeric = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
                return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), numeric) : numeric;
            }
            case "RECORD":
            case "STRUCT":
                final Schema structSchema = convertSchema(schema.getFields());
                return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), structSchema) : structSchema;
            default:
                throw new IllegalArgumentException("BigQuery TableSchema type: " + schema.toString() + " is not supported!");
        }
    }

}
