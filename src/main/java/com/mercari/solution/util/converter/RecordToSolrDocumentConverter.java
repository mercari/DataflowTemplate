package com.mercari.solution.util.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SolrSchemaUtil;
import com.mercari.solution.util.XmlUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class RecordToSolrDocumentConverter {

    public static SolrInputDocument convert(final GenericRecord record, final List<String> fieldNames) {
        return convert(record.getSchema(), record, fieldNames);
    }

    public static SolrInputDocument convert(final SchemaAndRecord schemaAndRecord) {
        if(schemaAndRecord.getTableSchema() == null) {
            return convert(schemaAndRecord.getRecord().getSchema(), schemaAndRecord.getRecord(), null);
        }
        final Schema schema = AvroSchemaUtil.convertSchema(schemaAndRecord.getTableSchema());
        return convert(schema, schemaAndRecord.getRecord(), null);
    }

    public static SolrInputDocument convert(final Schema schema, final GenericRecord record, final List<String> fieldNames) {
        return convert(schema, record, null, fieldNames);
    }

    public static SolrInputDocument convert(final Schema schema, final GenericRecord record, final String parentName, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final Schema.Field field : schema.getFields()) {
            if(fieldNames != null && !fieldNames.contains(field.name())) {
                continue;
            }
            setFieldValue(doc, parentName, field.name(), field.schema(), record, fieldNames);
        }
        return doc;
    }

    public static String convertSchema(final Schema schema) {
        final Document document = SolrSchemaUtil.createSchemaXML("content");

        // fields
        final Element fields = document.createElement("fields");
        for(final Schema.Field field : schema.getFields()) {
            setSchemaField(document, fields, field.name(), field.schema(), true, false);
        }

        // types
        final Element types = document.createElement("types");
        SolrSchemaUtil.setDefaultSchemaTypes(document, types);

        final Element root = document.getDocumentElement();
        root.appendChild(fields);
        root.appendChild(types);
        return XmlUtil.toString(document);
    }

    private static void setFieldValue(final SolrInputDocument doc,
                                      final String parentName, final String fieldName,
                                      final Schema schema, final GenericRecord record,
                                      final List<String> fieldNames) {

        final boolean isNullField = record.get(fieldName) == null;
        final Object value = record.get(fieldName);
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        switch (schema.getType()) {
            case BOOLEAN:
                doc.addField(name, isNullField ? false : (Boolean)value);
                return;
            case ENUM:
                doc.addField(name, isNullField ? "" : value.toString());
                return;
            case STRING:
                doc.addField(name, isNullField ? "" : value.toString());
                return;
            case FIXED:
                doc.addField(name, isNullField ? new byte[0] : ((GenericData.Fixed)value).bytes());
                return;
            case BYTES:
                doc.addField(name, isNullField ? new byte[0] : ((ByteBuffer)value).array());
                return;
            case INT: {
                final Integer intValue = (Integer) value;
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(intValue == null) {
                        doc.addField(name, "");
                    } else {
                        final LocalDate ld = LocalDate.ofEpochDay(intValue);
                        final Date date = Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
                        doc.addField(name, date);
                    }
                    return;
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    doc.addField(name, isNullField ? "" : new Date(intValue).toString());
                    return;
                } else {
                    doc.addField(name, isNullField ? 0 : intValue);
                    return;
                }
            }
            case LONG: {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    final Date date = isNullField ? new Date(0) : new Date(longValue);
                    doc.addField(name, date);
                    return;
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Date date = isNullField ? new Date(0) : new Date(longValue / 1000L);
                    doc.addField(name, date);
                    return;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    //TODO
                    doc.addField(name, isNullField ? 0L : longValue);
                    return;
                } else {
                    doc.addField(name, isNullField ? 0L : longValue);
                    return;
                }
            }
            case FLOAT:
                doc.addField(name, isNullField ? 0F : (Float)value);
                return;
            case DOUBLE:
                doc.addField(name, isNullField ? 0D : (Double)value);
                return;
            case RECORD:
                if(!isNullField) {
                    doc.addChildDocument(convert(schema, (GenericRecord) value, name, fieldNames));
                }
                return;
            case ARRAY:
                setArrayFieldValue(doc, parentName, name, schema.getElementType(), record, fieldNames);
                return;
            case UNION:
                setFieldValue(doc, parentName, name, AvroSchemaUtil.unnestUnion(schema), record, fieldNames);
                return;
            default:
                return;
        }
    }

    private static void setArrayFieldValue(final SolrInputDocument doc,
                                           final String parentName, final String fieldName,
                                           final Schema schema, final GenericRecord record,
                                           final List<String> fieldNames) {

        if(record.get(fieldName) == null) {
            return;
        }
        final Object value = record.get(fieldName);
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        switch (schema.getType()) {
            case BOOLEAN:
                ((List<Boolean>)value).stream()
                        .filter(Objects::nonNull)
                        .forEach(b -> doc.addField(name, b));
                return;
            case ENUM:
                ((List<Object>)value).stream()
                        .filter(Objects::nonNull)
                        .map(o -> o.toString())
                        .forEach(s -> doc.addField(name, s));
                return;
            case STRING:
                ((List<Object>) value).stream()
                        .filter(Objects::nonNull)
                        .map(utf8 -> utf8.toString())
                        .forEach(s -> doc.addField(name, s));
                return;
            case FIXED:
                ((List<GenericData.Fixed>)value).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.bytes())
                        .forEach(s -> doc.addField(name, s));
                return;
            case BYTES:
                ((List<ByteBuffer>) value).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.array())
                        .forEach(b -> doc.addField(name, b));
                return;
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    ((List<Integer>) value).stream()
                            .filter(Objects::nonNull)
                            .map(LocalDate::ofEpochDay)
                            .map(LocalDate::atStartOfDay)
                            .map(ldt -> ldt.toInstant(ZoneOffset.UTC))
                            .map(Date::from)
                            .forEach(d -> doc.addField(name, d));
                    return;
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    ((List<Integer>) value).stream()
                            .filter(Objects::nonNull)
                            .map(second -> new Long(second) * 1000 * 1000)
                            .map(ms -> LocalTime.ofNanoOfDay(ms).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .forEach(s -> doc.addField(name, s));
                    return;
                }
                ((List<Integer>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
                return;
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    ((List<Long>) value).stream()
                            .filter(Objects::nonNull)
                            .map(Date::new)
                            .forEach(s -> doc.addField(name, s));
                    return;
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    ((List<Long>) value).stream()
                            .filter(Objects::nonNull)
                            .map(l -> l / 1000)
                            .map(Date::new)
                            .forEach(s -> doc.addField(name, s));
                    return;
                }
                ((List<Long>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
                return;
            case FLOAT:
                ((List<Float>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
                return;
            case DOUBLE:
                ((List<Double>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
                return;
            case RECORD:
                ((List<GenericRecord>) value).stream()
                        .filter(Objects::nonNull)
                        .forEach(r -> doc.addChildDocument(convert(schema, r, name, fieldNames)));
                return;
            case UNION:
                setArrayFieldValue(doc, parentName, fieldName, AvroSchemaUtil.unnestUnion(schema), record, fieldNames);
                return;
        }
    }

    private static void setSchemaField(final Document document,
                                       final Element fields,
                                       final String name,
                                       final Schema schema,
                                       final boolean nullable,
                                       final boolean repeated) {

        final Element fieldElement = document.createElement("field");
        fieldElement.setAttribute("name", name);
        fieldElement.setAttribute("indexed", "true");
        fieldElement.setAttribute("stored", "true");
        fieldElement.setAttribute("required", nullable ? "false" : "true");
        if(repeated) {
            fieldElement.setAttribute("multiValued", "true");
        }
        switch (schema.getType()) {
            case ENUM: {
                fieldElement.setAttribute("type", "string");
                break;
            }
            case STRING: {
                fieldElement.setAttribute("type", "textja");
                break;
            }
            case BOOLEAN: {
                fieldElement.setAttribute("type", "boolean");
                break;
            }
            case INT: {
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "date");
                    break;
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "string");
                    break;
                } else {
                    fieldElement.setAttribute("type", "int");
                    break;
                }
            }
            case LONG: {
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "date");
                    break;
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "date");
                    break;
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    fieldElement.setAttribute("type", "string");
                    break;
                } else {
                    fieldElement.setAttribute("type", "long");
                    break;
                }
            }
            case FLOAT: {
                fieldElement.setAttribute("type", "float");
                break;
            }
            case DOUBLE: {
                fieldElement.setAttribute("type", "double");
                break;
            }
            case ARRAY: {
                setSchemaField(document, fields, name, schema.getElementType(), nullable, true);
                return;
            }
            case UNION: {
                setSchemaField(document, fields, name, AvroSchemaUtil.unnestUnion(schema), true, repeated);
                return;
            }
            case RECORD:
            case FIXED:
            case BYTES:
            case MAP:
            case NULL:
            default:
                return;
        }
        fields.appendChild(fieldElement);
    }

}
