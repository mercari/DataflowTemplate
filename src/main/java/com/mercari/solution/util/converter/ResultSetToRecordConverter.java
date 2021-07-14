package com.mercari.solution.util.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.compress.utils.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class ResultSetToRecordConverter {

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    public static Schema convertSchema(final ResultSet resultSet)  throws SQLException {

        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();

        final ResultSetMetaData meta = resultSet.getMetaData();
        final int columnCount = meta.getColumnCount();
        for (int column = 1; column <= columnCount; ++column) {
            final String fieldName = meta.getColumnName(column);
            switch (meta.getColumnType(column)) {
                case Types.BIT:
                case Types.BOOLEAN:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault();
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    schemaFields.name(fieldName).type(Schema
                            .createUnion(
                                    LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)),
                                    Schema.create(Schema.Type.NULL)))
                            .noDefault();
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_INT).noDefault();
                    break;
                case Types.BIGINT:
                    if("java.math.BigInteger".equals(meta.getColumnClassName(column))) {
                        schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_LONG).noDefault();
                        /*
                        schemaFields.name(fieldName).type(Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.decimal(meta.getPrecision(column), meta.getScale(column))
                                        .addToSchema(Schema.create(Schema.Type.BYTES)))).noDefault();
                        */
                    } else {
                        schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_LONG).noDefault();
                    }
                    break;
                case Types.REAL:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault();
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault();
                    break;
                case Types.OTHER:
                case Types.CHAR:
                case Types.ROWID:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
                    break;
                case Types.TIME:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE).noDefault();
                    break;
                case Types.DATE:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault();
                    break;
                case Types.TIMESTAMP:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault();
                    break;
                case Types.JAVA_OBJECT:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_BYTES).noDefault();
                    break;
                case Types.TIME_WITH_TIMEZONE:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_SQL_DATETIME_TYPE).noDefault();
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    schemaFields.name(fieldName).type(AvroSchemaUtil.NULLABLE_SQL_DATETIME_TYPE).noDefault();
                    break;
                case Types.STRUCT:
                case Types.ARRAY:
                case Types.CLOB:
                case Types.REF:
                case Types.SQLXML:
                case Types.NCLOB:
                case Types.REF_CURSOR:
                case Types.DISTINCT:
                case Types.DATALINK:
                case Types.NULL: {
                    break;
                }
            }
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord convert(final ResultSet resultSet) throws SQLException, IOException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int columnCount = meta.getColumnCount();
        final Schema schema = convertSchema(resultSet);
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (int column = 1; column <= columnCount; ++column) {
            final String fieldName = meta.getColumnName(column);
            switch (meta.getColumnType(column)) {
                case Types.BIT:
                case Types.BOOLEAN:
                    builder.set(fieldName, resultSet.getBoolean(column));
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL: {
                    final BigDecimal decimal = resultSet.getBigDecimal(column);
                    if (decimal == null) {
                        builder.set(fieldName, null);
                    } else if(decimal.scale() > 9) {
                        final BigDecimal newDecimal = decimal
                                .setScale(9, RoundingMode.HALF_UP)
                                .scaleByPowerOfTen(9);
                        builder.set(fieldName, ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray()));
                    } else {
                        final BigDecimal newDecimal = decimal.scaleByPowerOfTen(9);
                        builder.set(fieldName, ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray()));
                    }
                    break;
                }
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    builder.set(fieldName, resultSet.getInt(column));
                    break;
                case Types.BIGINT:
                    if("java.math.BigInteger".equals(meta.getColumnClassName(column))) {
                        final BigDecimal bigintDecimal = resultSet.getBigDecimal(column);
                        if(bigintDecimal == null) {
                            builder.set(fieldName, null);
                        } else {
                            builder.set(fieldName, bigintDecimal.unscaledValue().longValue());
                        }
                    } else {
                        builder.set(fieldName, resultSet.getLong(column));
                    }
                    break;
                case Types.REAL:
                    builder.set(fieldName, resultSet.getFloat(column));
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    builder.set(fieldName, resultSet.getDouble(column));
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.OTHER:
                    builder.set(fieldName, resultSet.getString(column));
                    break;
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                    builder.set(fieldName, resultSet.getNString(column));
                    break;
                case Types.ROWID: {
                    final RowId rowId = resultSet.getRowId(column);
                    if (rowId == null) {
                        builder.set(fieldName, null);
                    } else {
                        builder.set(fieldName, rowId.toString());
                    }
                    break;
                }
                case Types.JAVA_OBJECT:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY: {
                    byte[] binary = resultSet.getBytes(column);
                    if (binary == null) {
                        builder.set(fieldName, null);
                    } else {
                        builder.set(fieldName, ByteBuffer.wrap(binary));
                    }
                    break;
                }
                case Types.BLOB: {
                    final Blob blob = resultSet.getBlob(column);
                    if (blob == null) {
                        builder.set(fieldName, null);
                    } else {
                        byte[] bytes = IOUtils.toByteArray(blob.getBinaryStream());
                        builder.set(fieldName, ByteBuffer.wrap(bytes));
                    }
                    break;
                }
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE: {
                    final Time time = resultSet.getTime(column);
                    if (time == null) {
                        builder.set(fieldName, null);
                    } else {
                        builder.set(fieldName, time.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
                    }
                    break;
                }
                case Types.DATE: {
                    final java.sql.Date sqlDate = resultSet.getDate(column);
                    if (sqlDate == null) {
                        builder.set(fieldName, null);
                    } else {
                        final LocalDate localDate = sqlDate.toLocalDate();
                        final DateTime datetime = new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                        final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                        builder.set(fieldName, days.getDays());
                    }
                    break;
                }
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE: {
                    final java.sql.Timestamp timestamp = resultSet.getTimestamp(column);
                    if (timestamp == null) {
                        builder.set(fieldName, null);
                    } else {
                        builder.set(fieldName, timestamp.getTime() * 1000);
                    }
                    break;
                }
                case Types.DATALINK:
                case Types.DISTINCT:
                case Types.STRUCT:
                case Types.ARRAY:
                case Types.CLOB:
                case Types.REF:
                case Types.SQLXML:
                case Types.NCLOB:
                case Types.REF_CURSOR:
                default: {
                    break;
                }
            }
        }
        return builder.build();
    }

}
