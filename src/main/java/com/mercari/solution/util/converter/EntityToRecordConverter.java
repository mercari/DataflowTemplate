package com.mercari.solution.util.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.Timestamp;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EntityToRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(EntityToRecordConverter.class);

    private static final Schema KEY_SCHEMA = SchemaBuilder
            .record("__key__").fields()
            .requiredString("namespace")
            .requiredString("app")
            .requiredString("path")
            .requiredString("kind")
            .optionalString("name")
            .optionalLong("id")
            .endRecord();

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);
    private static final DateTimeFormatter FORMAT_TIME1 = DateTimeFormat.forPattern("HHmm");
    private static final DateTimeFormatter FORMAT_TIME2 = DateTimeFormat.forPattern("HH:mm");
    private static final DateTimeFormatter FORMAT_TIME3 = DateTimeFormat.forPattern("HH:mm:ss");
    private static final Pattern PATTERN_DATE1 = Pattern.compile("[0-9]{8}");
    private static final Pattern PATTERN_DATE2 = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final Pattern PATTERN_DATE3 = Pattern.compile("[0-9]{4}/[0-9]{2}/[0-9]{2}");
    private static final Pattern PATTERN_TIME1 = Pattern.compile("[0-9]{4}");
    private static final Pattern PATTERN_TIME2 = Pattern.compile("[0-9]{2}:[0-9]{2}");
    private static final Pattern PATTERN_TIME3 = Pattern.compile("[0-9]{4}:[0-9]{2}:[0-9]{2}");

    private EntityToRecordConverter() {}

    private static Schema addKeyToSchema(final String schemaString) {
        try {
            final Schema schema = new Schema.Parser().parse(schemaString);
            return addKeyToSchema(schema);
        } catch (SchemaParseException e) {
            LOG.error(String.format("Failed to parse schema: %s", schemaString));
            throw e;
        }
    }

    public static Schema addKeyToSchema(final Schema schema) {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
                .record(schema.getName())
                .namespace(schema.getNamespace())
                .fields().name("__key__").type(KEY_SCHEMA).noDefault();
        schema.getFields().stream().forEach(f -> {
            if(f.defaultVal() == null) {
                builder.name(f.name()).type(f.schema()).noDefault();
            } else if(f.defaultVal().equals(JsonProperties.NULL_VALUE)) {
                builder.name(f.name()).type(f.schema()).withDefault(null);
            } else {
                builder.name(f.name()).type(f.schema()).withDefault(f.defaultVal());
            }
        });
        return builder.endRecord();
    }

    public static GenericRecord convert(final AvroWriteRequest<Entity> request) {
        return convert(request.getSchema(), request.getElement());
    }


    public static GenericRecord convert(final Schema schema, final Entity entity) {
        return convert(schema, entity, 0);
    }

    private static GenericRecord convert(final Schema schema, final Entity entity, final int depth) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null! " + entity.getKey());
        }
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        if(depth == 0) {
            try {
                if (schema.getField("__key__") != null) {
                    builder.set("__key__", convertKeyRecord(entity.getKey()));
                }
            } catch (NullPointerException e) {
                throw new RuntimeException(convertKeyRecord(entity.getKey()).toString(), e);
            }
        }
        for(final Schema.Field field : schema.getFields()) {
            if(field.name().equals("__key__")) {
                continue;
            }
            final Value value = getValue(field, entity);
            builder.set(field, convertEntityValue(value, field.schema(), depth + 1));
        }
        return builder.build();
    }

    private static Value getValue(final Schema.Field field, final Entity entity) {
        if (entity == null) {
            return null;
        }

        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(field.name()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private static Object convertEntityValue(final Value value, final Schema schema, final int depth) {
        if (value == null || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
            return null;
        }

        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return value.getStringValue();
            case FIXED:
            case BYTES:
                return value.getBlobValue().asReadOnlyByteBuffer();
            case BOOLEAN:
                return value.getBooleanValue();
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(Value.ValueTypeCase.STRING_VALUE.equals(value.getValueTypeCase())) {
                        final String datestr = value.getStringValue();
                        if(PATTERN_DATE1.matcher(datestr).find()) {
                            return Days.daysBetween(EPOCH_DATETIME, new DateTime(
                                    Integer.valueOf(datestr.substring(0, 4)),
                                    Integer.valueOf(datestr.substring(4, 6)),
                                    Integer.valueOf(datestr.substring(6, 8)),
                                    0, 0, DateTimeZone.UTC)).getDays();
                        } else if(PATTERN_DATE2.matcher(datestr).find() || PATTERN_DATE3.matcher(datestr).find()) {
                            return Days.daysBetween(EPOCH_DATETIME, new DateTime(
                                    Integer.valueOf(datestr.substring(0, 4)),
                                    Integer.valueOf(datestr.substring(5, 7)),
                                    Integer.valueOf(datestr.substring(8, 10)),
                                    0, 0, DateTimeZone.UTC)).getDays();
                        } else {
                            throw new IllegalArgumentException("Illegal date string: " + datestr);
                        }
                    } else if(Value.ValueTypeCase.INTEGER_VALUE.equals(value.getValueTypeCase())) {
                        return value.getIntegerValue();
                    } else {
                        throw new IllegalArgumentException();
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final String timestr = value.getStringValue();
                    if(PATTERN_TIME1.matcher(timestr).find()) {
                        return FORMAT_TIME1.parseLocalTime(timestr).getMillisOfDay();
                    } else if(PATTERN_TIME2.matcher(timestr).find()) {
                        return FORMAT_TIME2.parseLocalTime(timestr).getMillisOfDay();
                    } else if(PATTERN_TIME3.matcher(timestr).find()) {
                        return FORMAT_TIME3.parseLocalTime(timestr).getMillisOfDay();
                    } else {
                        throw new IllegalArgumentException("Illegal time string: " + timestr);
                    }
                } else {
                    return value.getIntegerValue();
                }
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    final Timestamp timestamp = value.getTimestampValue();
                    return timestamp.getSeconds() * 1000;
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Timestamp timestamp = value.getTimestampValue();
                    return timestamp.getSeconds() * 1000000;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return value.getIntegerValue();
                } else {
                    return value.getIntegerValue();
                }
            case FLOAT:
            case DOUBLE:
                return value.getDoubleValue();
            case RECORD:
                return convert(schema, value.getEntityValue(), depth);
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> convertEntityValue(v, schema.getElementType(), depth))
                        .collect(Collectors.toList());
            case UNION:
                final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                return convertEntityValue(value, childSchema, depth);
            case MAP:
            case NULL:
            default:
                return null;
        }
    }

    private static GenericRecord convertKeyRecord(final Key key) {
        if(key.getPathCount() == 0) {
            throw new RuntimeException("PathList size must not be zero! " + key.toString() + " " + key.getPathList().size());
        }
        final Key.PathElement lastPath = key.getPath(key.getPathCount() - 1);
        final String path = key.getPathList().stream()
                .map(e -> String.format("\"%s\", %s", e.getKind(), (e.getName() == null || e.getName().length() == 0) ?
                        Long.toString(e.getId()) : String.format("\"%s\"", e.getName())))
                .collect(Collectors.joining(", "));
        return new GenericRecordBuilder(KEY_SCHEMA)
                .set("namespace", key.getPartitionId().getNamespaceId())
                .set("app", key.getPartitionId().getProjectId())
                .set("path", path)
                .set("kind", lastPath.getKind())
                .set("name", lastPath.getName() == null || lastPath.getName().length() == 0 ? null : lastPath.getName())
                .set("id", lastPath.getId() == 0 ? null : lastPath.getId())
                .build();
    }

}
