package com.mercari.solution.util.converter;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.SolrUtil;
import com.mercari.solution.util.XmlUtil;
import org.apache.solr.common.SolrInputDocument;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

public class StructToSolrDocumentConverter {

    public static SolrInputDocument convert(final Struct struct, final List<String> fieldNames) {
        return convert(struct, null, fieldNames);
    }

    public static SolrInputDocument convert(final Struct struct, final String parentName, final List<String> fieldNames) {
        final SolrInputDocument doc = new SolrInputDocument();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(fieldNames != null && !fieldNames.contains(field.getName())) {
                continue;
            }
            setFieldValue(doc, parentName, field.getName(), field.getType(), struct, fieldNames);
        }
        return doc;
    }

    public static String convertSchema(final Type type) {
        final Document document = SolrUtil.createSchemaXML("content");

        // fields
        final Element fields = document.createElement("fields");
        for(final Type.StructField field : type.getStructFields()) {
            setSchemaField(document, fields, field.getName(), field.getType(), true, false);
        }

        // types
        final Element types = document.createElement("types");
        SolrUtil.setDefaultSchemaTypes(document, types);

        final Element root = document.getDocumentElement();
        root.appendChild(fields);
        root.appendChild(types);
        return XmlUtil.toString(document);
    }

    private static void setFieldValue(final SolrInputDocument doc,
                                      final String parentName, final String fieldName,
                                      final Type type, final Struct struct,
                                      final List<String> fieldNames) {

        final boolean isNullField = struct.isNull(fieldName);
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        switch (type.getCode()) {
            case BOOL: {
                doc.addField(name, !isNullField && struct.getBoolean(fieldName));
                return;
            }
            case STRING: {
                doc.addField(name, isNullField ? "" : struct.getString(fieldName));
                return;
            }
            case BYTES: {
                doc.addField(name, isNullField ? new byte[0] : struct.getBytes(fieldName).toByteArray());
                return;
            }
            case INT64: {
                doc.addField(name, isNullField ? 0L : struct.getLong(fieldName));
                return;
            }
            case FLOAT64: {
                doc.addField(name, isNullField ? 0D : struct.getDouble(fieldName));
                return;
            }
            case DATE: {
                final com.google.cloud.Date date = struct.getDate(fieldName);
                final LocalDate ld = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                doc.addField(name, isNullField ? "" : java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC)));
                return;
            }
            case TIMESTAMP: {
                doc.addField(name, isNullField ? "" : struct.getTimestamp(fieldName).toDate());
                return;
            }
            case STRUCT:
                if(!isNullField) {
                    doc.addChildDocument(convert(struct.getStruct(fieldName), name, fieldNames));
                }
                return;
            case ARRAY:
                setArrayFieldValue(doc, parentName, name, struct.getType().getArrayElementType(), struct, fieldNames);
                return;
            case NUMERIC:
            default:
                return;
        }
    }

    private static void setArrayFieldValue(final SolrInputDocument doc,
                                           final String parentName, final String fieldName,
                                           final Type type, final Struct struct,
                                           final List<String> fieldNames) {

        if(struct.isNull(fieldName)) {
            return;
        }
        final String name = parentName == null ? fieldName : parentName + "." + fieldName;
        switch (type.getCode()) {
            case BOOL: {
                struct.getBooleanList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(b -> doc.addField(name, b));
                return;
            }
            case STRING: {
                struct.getStringList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(s -> doc.addField(name, s));
                return;
            }
            case BYTES: {
                struct.getBytesList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.toByteArray())
                        .forEach(b -> doc.addField(name, b));
                return;
            }
            case INT64: {
                struct.getLongList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
                return;
            }
            case FLOAT64: {
                struct.getDoubleList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .forEach(i -> doc.addField(name, i));
                return;
            }
            case DATE: {
                struct.getDateList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(d -> {
                            final com.google.cloud.Date date = struct.getDate(fieldName);
                            final LocalDate ld = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                            return java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
                        })
                        .forEach(s -> doc.addField(name, s));
                return;
            }
            case TIMESTAMP: {
                struct.getTimestampList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(Timestamp::toDate)
                        .forEach(i -> doc.addField(name, i));
                return;
            }
            case STRUCT: {
                struct.getStructList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(s -> convert(s, name, fieldNames))
                        .forEach(doc::addChildDocument);
                return;
            }
            case NUMERIC:
            case ARRAY:
            default:
                return;
        }
    }

    private static void setSchemaField(final Document document,
                                       final Element fields,
                                       final String name,
                                       final Type type,
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
        switch (type.getCode()) {
            case STRING: {
                fieldElement.setAttribute("type", "textja");
                break;
            }
            case BOOL: {
                fieldElement.setAttribute("type", "boolean");
                break;
            }
            case INT64: {
                fieldElement.setAttribute("type", "long");
                break;
            }
            case FLOAT64: {
                fieldElement.setAttribute("type", "double");
                break;
            }
            case DATE: {
                fieldElement.setAttribute("type", "date");
                break;
            }
            case TIMESTAMP: {
                fieldElement.setAttribute("type", "date");
                break;
            }
            case ARRAY: {
                setSchemaField(document, fields, name, type.getArrayElementType(), nullable, true);
                return;
            }
            case STRUCT:
            case BYTES:
            case NUMERIC:
            default:
                return;
        }
        fields.appendChild(fieldElement);
    }

}
