package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

public class ToStatementConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ToStatementConverter.class);

    public static void convertRecord(final GenericRecord record, final PreparedStatement statement) throws SQLException {
        convertRecordWithKeys(record, statement, null);
    }

    public static void convertRecordWithKeys(
            final GenericRecord record, final PreparedStatement statement,
            final List<String> keyFields) throws SQLException {

        int index = 1;
        for(final org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            final boolean isNull = record.get(field.name()) == null;
            final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(field.name())) {
                continue;
            }
            switch (fieldSchema.getType()) {
                case BOOLEAN:
                    if(isNull) {
                        statement.setNull(index, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(index, ((Boolean) record.get(field.name())));
                    }
                    break;
                case FIXED:
                case BYTES: {
                    if (AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                        if (isNull) {
                            statement.setNull(index, Types.DECIMAL);
                        } else {
                            final byte[] bytes = ((ByteBuffer) record.get(field.name())).array();
                            statement.setBigDecimal(index, new BigDecimal(new BigInteger(1, bytes)));
                        }
                    } else {
                        if (isNull) {
                            statement.setNull(index, Types.BINARY);
                        } else {
                            final byte[] bytes = ((ByteBuffer) record.get(field.name())).array();
                            statement.setBytes(index, bytes);
                        }
                    }
                    break;
                }
                case ENUM:
                case STRING:
                    if(isNull) {
                        statement.setNull(index, Types.VARCHAR);
                    } else {
                        statement.setString(index, (record.get(field.name())).toString());
                    }
                    break;
                case INT: {
                    final Integer i = (Integer) record.get(field.name());
                    if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                        if(isNull) {
                            statement.setNull(index, Types.DATE);
                        } else {
                            statement.setDate(index, Date.valueOf(LocalDate.ofEpochDay(i)));
                        }
                    } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                        if(isNull) {
                            statement.setNull(index, Types.TIME);
                        } else {
                            statement.setTime(index, Time.valueOf(LocalTime.ofNanoOfDay(i * 1000 * 1000)));
                        }
                    } else {
                        if(isNull) {
                            statement.setNull(index, Types.INTEGER);
                        } else {
                            statement.setInt(index, i);
                        }
                    }
                    break;
                }
                case LONG: {
                    final Long i = (Long) record.get(field.name());
                    if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                        if(isNull) {
                            statement.setNull(index, Types.TIMESTAMP);
                        } else {
                            statement.setTimestamp(index, Timestamp.from(Instant.ofEpochMilli(i)));
                        }
                    } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                        if(isNull) {
                            statement.setNull(index, Types.TIMESTAMP);
                        } else {
                            statement.setTimestamp(index, Timestamp.from(Instant.ofEpochMilli(i / 1000)));
                        }
                    } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                        if(isNull) {
                            statement.setNull(index, Types.TIME);
                        } else {
                            statement.setTime(index, Time.valueOf(LocalTime.ofNanoOfDay(i * 1000)));
                        }
                    } else {
                        if(isNull) {
                            statement.setNull(index, Types.BIGINT);
                        } else {
                            statement.setLong(index, i);
                        }
                    }
                    break;
                }
                case FLOAT:
                    if(isNull) {
                        statement.setNull(index, Types.REAL);
                    } else {
                        statement.setFloat(index, ((Float) record.get(field.name())));
                    }
                    break;
                case DOUBLE:
                    if(isNull) {
                        statement.setNull(index, Types.DOUBLE);
                    } else {
                        statement.setDouble(index, ((Double) record.get(field.name())));
                    }
                    break;
                case ARRAY:
                case MAP:
                case RECORD:
                case UNION:
                case NULL:
                default:
                    break;
            }
            index++;
        }
    }
    public static void convertRow(final Row row, final PreparedStatement statement) throws SQLException {
        convertRowWithKeys(row, statement, null);
    }

    public static void convertRowWithKeys(final Row row, final PreparedStatement statement,
                                          final List<String> keyFields) throws SQLException {

        int index = 1;
        for(final Schema.Field field : row.getSchema().getFields()) {
            final boolean isNull = row.getValue(field.getName()) == null;
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(field.getName())) {
                continue;
            }
            switch (field.getType().getTypeName()) {
                case BOOLEAN:
                    if(isNull) {
                        statement.setNull(index, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(index, row.getBoolean(field.getName()));
                    }
                    break;
                case INT16:
                    if(isNull) {
                        statement.setNull(index, Types.SMALLINT);
                    } else {
                        statement.setShort(index, row.getInt16(field.getName()));
                    }
                    break;
                case INT32:
                    if(isNull) {
                        statement.setNull(index, Types.INTEGER);
                    } else {
                        statement.setInt(index, row.getInt32(field.getName()));
                    }
                    break;
                case INT64:
                    if(isNull) {
                        statement.setNull(index, Types.BIGINT);
                    } else {
                        statement.setLong(index, row.getInt64(field.getName()));
                    }
                    break;
                case FLOAT:
                    if(isNull) {
                        statement.setNull(index, Types.REAL);
                    } else {
                        statement.setFloat(index, row.getFloat(field.getName()));
                    }
                    break;
                case DOUBLE:
                    if(isNull) {
                        statement.setNull(index, Types.DOUBLE);
                    } else {
                        statement.setDouble(index, row.getDouble(field.getName()));
                    }
                    break;
                case BYTES:
                    if(isNull) {
                        statement.setNull(index, Types.BINARY);
                    } else {
                        statement.setBytes(index, row.getBytes(field.getName()));
                    }
                    break;
                case STRING:
                    if(isNull) {
                        statement.setNull(index, Types.VARCHAR);
                    } else {
                        statement.setString(index, row.getString(field.getName()));
                    }
                    break;
                case DATETIME:
                    if(isNull) {
                        statement.setNull(index, Types.TIMESTAMP);
                    } else {
                        statement.setTimestamp(index, Timestamp.from(Instant.ofEpochMilli(row.getDateTime(field.getName()).getMillis())));
                    }
                    break;
                case LOGICAL_TYPE:
                    if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                        if(isNull) {
                            statement.setNull(index, Types.DATE);
                        } else {
                            final LocalDate localDate = row.getLogicalTypeValue(field.getName(), LocalDate.class);
                            statement.setDate(index, Date.valueOf(localDate));
                        }
                    } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                        if(isNull) {
                            statement.setNull(index, Types.TIME);
                        } else {
                            org.joda.time.Instant instant = row.getLogicalTypeValue(field.getName(), org.joda.time.Instant.class);
                            statement.setTime(index, Time.valueOf(LocalTime.ofSecondOfDay(instant.toDateTime().getSecondOfDay())));
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "Unsupported Beam logical type: " + field.getType().getLogicalType().getIdentifier());
                    }
                    break;
                case DECIMAL:
                case BYTE:
                case MAP:
                case ROW:
                case ITERABLE:
                case ARRAY:
                default:
                    break;
            }
            index++;
        }
    }

    public static void convertStruct(final Struct struct, final PreparedStatement statement) throws SQLException {
        convertStructWithKeys(struct, statement, null);
    }

    public static void convertStructWithKeys(final Struct struct, final PreparedStatement statement,
                                             final List<String> keyFields) throws SQLException {
        int index = 1;
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(keyFields != null && keyFields.size() > 0 && !keyFields.contains(field.getName())) {
                continue;
            }
            switch (field.getType().getCode()) {
                case BOOL:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.BOOLEAN);
                    } else {
                        statement.setBoolean(index, struct.getBoolean(field.getName()));
                    }
                    break;
                case STRING:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.VARCHAR);
                    } else {
                        statement.setString(index, struct.getString(field.getName()));
                    }
                    break;
                case BYTES:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.BINARY);
                    } else {
                        statement.setBytes(index, struct.getBytes(field.getName()).toByteArray());
                    }
                    break;
                case INT64:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.BIGINT);
                    } else {
                        statement.setLong(index, struct.getLong(field.getName()));
                    }
                    break;
                case FLOAT64:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.DOUBLE);
                    } else {
                        statement.setDouble(index, struct.getDouble(field.getName()));
                    }
                    break;
                case DATE:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.DATE);
                    } else {
                        final com.google.cloud.Date date = struct.getDate(field.getName());
                        statement.setDate(index, Date.valueOf(LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth())));
                    }
                    break;
                case TIMESTAMP:
                    if(struct.isNull(field.getName())) {
                        statement.setNull(index, Types.TIMESTAMP);
                    } else {
                        statement.setTimestamp(index, struct.getTimestamp(field.getName()).toSqlTimestamp());
                    }
                    break;
                case STRUCT:
                case ARRAY:
                default:
                    break;
            }
            index++;
        }
    }

    public static void convertEntity(final Entity entity, final Statement statement) {

    }

}
