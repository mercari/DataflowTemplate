package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RecordToMutationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToMutationConverter.class);

    public static Type convertSchema(final Schema schema) {
        return convertFieldType(schema);
    }

    public static Mutation convert(final Schema schema, final GenericRecord record,
                                   final String table, final String mutationOp, final Iterable<String> keyFields,
                                   final Set<String> excludeFields, final Set<String> hideFields) {

        if(mutationOp != null && "DELETE".equals(mutationOp.trim().toUpperCase())) {
            return SpannerUtil.createDeleteMutation(record, table, keyFields, GenericRecord::get);
        }

        final Mutation.WriteBuilder builder = SpannerUtil.createMutationWriteBuilder(table, mutationOp);
        for(final Schema.Field field : schema.getFields()) {
            if(excludeFields != null && excludeFields.contains(field.name())) {
                continue;
            }
            final boolean hide = hideFields != null && hideFields.contains(field.name());
            final String fieldName = field.name();
            final Object value = record.get(fieldName);
            setValue(builder, fieldName, field.schema(), value, hide, false);
        }
        return builder.build();
    }

    public static MutationGroup convertGroup(final Schema schema, final GenericRecord record, final String mutationOp, final String primaryField) {
        Mutation primary = null;
        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : record.getSchema().getFields()) {
            final String fieldName = field.name();
            if(record.get(fieldName) == null) {
                continue;
            }
            switch (field.schema().getType()) {
                case BOOLEAN:
                case STRING:
                case BYTES:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case MAP:
                    break;
                case RECORD:
                    final Mutation mutation = convert(schema, record, fieldName, mutationOp, null, null, null);
                    if(field.name().equals(primaryField)) {
                        primary = mutation;
                    } else {
                        mutations.add(mutation);
                    }
                    break;
                case ARRAY: {
                    if (!Schema.Type.RECORD.equals(field.schema().getElementType().getType())) {
                        break;
                    }
                    final List<Mutation> mutationArray = ((List<GenericRecord>)record.get(fieldName)).stream()
                            .map(r -> convert(r.getSchema(), r, fieldName, mutationOp, null, null, null))
                            .collect(Collectors.toList());
                    if(mutationArray.size() == 0) {
                        break;
                    }
                    if(field.name().equals(primaryField)) {
                        primary = mutationArray.get(0);
                        //mutations.addAll(mutationArray.subList(1, mutationArray.size()));
                        mutations.addAll(mutationArray);
                    } else {
                        mutations.addAll(mutationArray);
                    }
                    break;
                }
                default:
                    break;
            }

        }
        if(primary == null) {
            return MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
        }
        LOG.error(primary.toString() + " : " + mutations.size());
        return MutationGroup.create(primary, mutations);
    }

    private static void setValue(final Mutation.WriteBuilder builder,
                                 final String fieldName, final Schema schema, final Object value,
                                 final boolean hide, final boolean nullableField) {

        final boolean isNullField = value == null;
        switch(schema.getType()) {
            case BOOLEAN:
                final Boolean booleanValue = hide ? (nullableField ? null : false) : (Boolean)value;
                builder.set(fieldName).to(booleanValue);
                break;
            case ENUM:
            case STRING:
                final String stringValue = hide ? (nullableField ? null : "") : isNullField ? null : value.toString();
                final String sqlType = schema.getProp("sqlType");
                if("DATETIME".equals(sqlType)) {
                    builder.set(fieldName).to(stringValue);
                } else if("GEOGRAPHY".equals(sqlType)) {
                    builder.set(fieldName).to(stringValue);
                } else {
                    builder.set(fieldName).to(stringValue);
                }
                break;
            case FIXED:
            case BYTES:
                final ByteArray bytesValue = hide ? (nullableField ? null : ByteArray.copyFrom(""))
                        : (isNullField ? null : ByteArray.copyFrom(((ByteBuffer)value).array()));
                builder.set(fieldName).to(bytesValue);
                break;
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    final Date dateValue = hide ? (nullableField ? null : Date.fromYearMonthDay(1970,1,1))
                            : convertEpochDaysToDate((Integer)value);
                    builder.set(fieldName).to(dateValue);
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final String timeValue = hide ? (nullableField ? null : "00:00:00") :
                            (isNullField ? null : LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                    builder.set(fieldName).to(timeValue);
                } else {
                    final Integer intValue = hide ? (nullableField ? null : 0) : (Integer) value;
                    builder.set(fieldName).to(intValue);
                }
                break;
            case LONG:
                final Long longValue = (Long)value;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    final Timestamp timestampValue = hide ? (nullableField ? null : Timestamp.MIN_VALUE)
                            : isNullField ? null : convertMicrosecToTimestamp(longValue * 1000);
                    builder.set(fieldName).to(timestampValue);
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    final Timestamp timestampValue = hide ? (nullableField ? null : Timestamp.MIN_VALUE)
                            : isNullField ? null : convertMicrosecToTimestamp(longValue);
                    builder.set(fieldName).to(timestampValue);
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    final String timeValue = hide ? (nullableField ? null : "00:00:00")
                            : isNullField ? null : convertNanosecToTimeString(longValue * 1000);
                    builder.set(fieldName).to(timeValue);
                } else {
                    builder.set(fieldName).to(hide ? (nullableField ? null : 0L) : longValue);
                }
                break;
            case FLOAT:
                final Float floatValue = hide ? (nullableField ? null : 0F) : (Float) value;
                builder.set(fieldName).to(floatValue);
                break;
            case DOUBLE:
                final Double doubleValue = hide ? (nullableField ? null : 0D) : (Double) value;
                builder.set(fieldName).to(doubleValue);
                break;
            case RECORD:
                // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                // https://cloud.google.com/spanner/docs/data-types
                break;
            case UNION:
                final boolean nullable = schema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(Schema.Type.NULL));
                final Schema unnested = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                setValue(builder, fieldName, unnested, value, hide, nullable);
                break;
            case ARRAY: {
                final List list = new ArrayList();
                final Schema elementSchema = AvroSchemaUtil.unnestUnion(schema.getElementType());
                switch (elementSchema.getType()) {
                    case BOOLEAN:
                        final List<Boolean> booleanList = hide || isNullField ? list :
                                ((List<Boolean>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toBoolArray(booleanList);
                        break;
                    case ENUM:
                    case STRING:
                        final List<String> stringList = hide || isNullField ? list :
                                ((List<Object>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .map(Object::toString)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toStringArray(stringList);
                        break;
                    case FIXED:
                    case BYTES:
                        final List<ByteArray> bytesList = hide || isNullField ? list :
                                ((List<ByteBuffer>) value).stream()
                                        .filter(Objects::nonNull)
                                        .map(ByteBuffer::array)
                                        .map(ByteArray::copyFrom)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toBytesArray(bytesList);
                        break;
                    case INT:
                        final List<Integer> intList = ((List<Integer>) value);
                        if (LogicalTypes.date().equals(elementSchema.getLogicalType())) {
                            final List<Date> dateList = hide || isNullField ? list :
                                    intList.stream()
                                            .filter(Objects::nonNull)
                                            .map(RecordToMutationConverter::convertEpochDaysToDate)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toDateArray(dateList);
                        } else if (LogicalTypes.timeMillis().equals(elementSchema.getLogicalType())) {
                            final List<String> timeList = hide || isNullField ? list :
                                    intList.stream()
                                            .filter(Objects::nonNull)
                                            .map(Long::new)
                                            .map(l -> l * 1000 * 1000)
                                            .map(LocalTime::ofNanoOfDay)
                                            .map(l -> l.format(DateTimeFormatter.ISO_LOCAL_TIME))
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toStringArray(timeList);
                        } else {
                            final List<Long> integerList = hide || isNullField ? list :
                                    intList.stream()
                                            .filter(Objects::nonNull)
                                            .map(Long::new)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toInt64Array(integerList);
                        }
                        break;
                    case LONG:
                        final List<Long> longList = (List<Long>) value;
                        if (LogicalTypes.timestampMillis().equals(elementSchema.getLogicalType())) {
                            final List<Timestamp> timestampList = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .map(l -> l * 1000)
                                            .map(RecordToMutationConverter::convertMicrosecToTimestamp)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toTimestampArray(timestampList);
                        } else if (LogicalTypes.timestampMicros().equals(elementSchema.getLogicalType())) {
                            final List<Timestamp> timestampList = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .map(RecordToMutationConverter::convertMicrosecToTimestamp)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toTimestampArray(timestampList);
                        } else if (LogicalTypes.timeMicros().equals(elementSchema.getLogicalType())) {
                            final List<String> timestampList = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .map(l -> l * 1000)
                                            .map(RecordToMutationConverter::convertNanosecToTimeString)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toStringArray(timestampList);
                        } else {
                            final List<Long> longLists = hide || isNullField ? list :
                                    longList.stream()
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toList());
                            builder.set(fieldName).toInt64Array(longLists);
                        }
                        break;
                    case FLOAT:
                        final List<Double> floatList = hide || isNullField ? list :
                                ((List<Float>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .map(Double::new)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toFloat64Array(floatList);
                        break;
                    case DOUBLE:
                        final List<Double> doubleList = hide || isNullField ? list :
                                ((List<Double>) value)
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());
                        builder.set(fieldName).toFloat64Array(doubleList);
                        break;
                    case RECORD:
                        // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                        // https://cloud.google.com/spanner/docs/data-types
                        break;
                    case ARRAY:
                        // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2019/03/04)
                        // https://cloud.google.com/spanner/docs/data-types
                        break;
                    case MAP:
                    case UNION:
                    case NULL:
                    default:
                        break;
                }
                break;
            }
            case MAP:
            case NULL:
            default:
                break;

        }
    }

    private static Type convertFieldType(final Schema schema) {
        switch (schema.getType()) {
            case BOOLEAN:
                return Type.bool();
            case ENUM:
                return Type.string();
            case STRING: {
                final String sqlType = schema.getProp("sqlType");
                if ("DATETIME".equals(sqlType)) {
                    return Type.string();
                } else if("GEOGRAPHY".equals(sqlType)) {
                    return Type.string();
                }
                return Type.string();
            }
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    return Type.string();
                }
                return Type.bytes();
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return Type.date();
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return Type.int64();
                }
                return Type.int64();
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Type.timestamp();
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Type.timestamp();
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return Type.int64();
                }
                return Type.int64();
            case FLOAT:
            case DOUBLE:
                return Type.float64();
            case RECORD:
                return Type.struct(schema.getFields().stream()
                        .map(f -> Type.StructField.of(f.name(), convertFieldType(f.schema())))
                        .collect(Collectors.toList()));
            case ARRAY:
                return Type.array(convertFieldType(schema.getElementType()));
            case UNION:
                final boolean nullable = schema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
                final org.apache.avro.Schema unnested = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(org.apache.avro.Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                return convertFieldType(unnested);
            case MAP:
            case NULL:
            default:
                return Type.string();
        }

    }

    private static Date convertEpochDaysToDate(final Integer epochDays) {
        if(epochDays == null) {
            return null;
        }
        final LocalDate ld = LocalDate.ofEpochDay(epochDays);
        return Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
    }

    private static String convertNanosecToTimeString(final Long nanos) {
        if(nanos == null) {
            return null;
        }
        final LocalTime localTime = LocalTime.ofNanoOfDay(nanos);
        return localTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    private static Timestamp convertMicrosecToTimestamp(final Long micros) {
        if(micros == null) {
            return null;
        }
        return Timestamp.ofTimeMicroseconds(micros);
    }

    private static String convertNumericBytesToString(final byte[] bytes, final int scale) {
        if(bytes == null) {
            return null;
        }
        final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        final StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

}
