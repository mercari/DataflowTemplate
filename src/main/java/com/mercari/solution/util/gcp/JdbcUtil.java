package com.mercari.solution.util.gcp;

import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.converter.ResultSetToRecordConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    public enum DB {
        MYSQL,
        POSTGRESQL,
        SQLSERVER
    }

    public enum OP {
        INSERT,
        INSERT_OR_UPDATE,
        INSERT_OR_DONOTHING,
        DELETE
    }

    private static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            "ALL","AND","ANY","ARRAY","AS","ASC","ASSERT_ROWS_MODIFIED","AT",
            "BETWEEN","BY","CASE","CAST","COLLATE","CONTAINS","CREATE","CROSS","CUBE","CURRENT",
            "DEFAULT","DEFINE","DESC","DISTINCT","ELSE","END","ENUM","ESCAPE","EXCEPT","EXCLUDE","EXISTS","EXTRACT",
            "FALSE","FETCH","FOLLOWING","FOR","FROM","FULL","GROUP","GROUPING","GROUPS","HASH","HAVING",
            "IF","IGNORE","IN","INNER","INTERSECT","INTERVAL","INTO","IS","JOIN",
            "LATERAL","LEFT","LIKE","LIMIT","LOOKUP","MERGE","NATURAL","NEW","NO","NOT","NULL","NULLS",
            "OF","ON","OR","ORDER","OUTER","OVER","PARTITION","PRECEDING","PROTO","RANGE");


    public static DataSource createDataSource(
            final String driverClassName,
            final String url,
            final String username,
            final String password) {

        return createDataSource(driverClassName, url, username, password, false);
    }

    public static DataSource createDataSource(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final boolean readOnly) {

        final BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(driverClassName);
        basicDataSource.setUrl(url);
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);

        // Wrapping the datasource as a pooling datasource
        final DataSourceConnectionFactory connectionFactory = new DataSourceConnectionFactory(basicDataSource);
        final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);

        final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMinIdle(0);
        poolConfig.setMinEvictableIdleTimeMillis(10000);
        poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
        final GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory, poolConfig);
        poolableConnectionFactory.setPool(connectionPool);
        poolableConnectionFactory.setDefaultAutoCommit(false);
        poolableConnectionFactory.setDefaultReadOnly(readOnly);
        return new PoolingDataSource(connectionPool);
    }

    public static Schema createAvroSchemaFromQuery(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final String query,
            final List<String> prepareCalls) throws Exception {

        final DataSource source = createDataSource(driverClassName, url, username, password, true);
        try(final Connection connection = source.getConnection()) {
            return createAvroSchemaFromQuery(connection, query, prepareCalls);
        }
    }

    public static Schema createAvroSchemaFromQuery(
            final Connection connection,
            final String query,
            final List<String> prepareCalls) throws Exception {

        for(final String prepareCall : prepareCalls) {
            try(final CallableStatement statement = connection
                    .prepareCall(prepareCall, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                final boolean result = statement.execute();
                if(result) {
                    LOG.info("Execute prepareCall: " + prepareCall);
                } else {
                    LOG.error("Failed execute prepareCall: " + prepareCall);
                }
            }
        }

        try(final PreparedStatement statement = connection
                .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            if(statement.getMetaData() == null) {
                throw new IllegalArgumentException("Failed to get schema for query: " + query);
            }

            return ResultSetToRecordConverter.convertSchema(statement.getMetaData());
        }
    }

    public static String buildCreateTableSQL(final Schema schema,
                                             final String table,
                                             final DB db,
                                             final List<String> keyFields) {

        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s (", table));
        schema.getFields().stream()
                .filter(f -> isValidColumnType(f.schema()))
                .forEach(f -> sb.append(String.format("%s %s%s,",
                        replaceReservedKeyword(f.name()),
                        getColumnType(f.schema(), db),
                        AvroSchemaUtil.isNullable(f.schema()) ? "" : " NOT NULL")));

        if(keyFields == null || keyFields.size() == 0) {
            sb.deleteCharAt(sb.length() - 1);
            sb.append(");");
        } else {
            final String primaryKey = keyFields.stream()
                    .map(JdbcUtil::replaceReservedKeyword)
                    .collect(Collectors.joining(","));
            sb.append(String.format(" PRIMARY KEY ( %s ));", primaryKey));
        }
        return sb.toString();
    }

    public static String createStatement(final String table, final Schema schema,
                                         final OP op, final DB db,
                                         final List<String> keyFields) {

        final StringBuilder sb;
        if(OP.DELETE.equals(op)) {
            /*
            sb = new StringBuilder("DELETE FROM " + table + " WHERE ");
            for(final String keyField : keyFields) {
                sb.append(keyField);
                sb.append("=? AND ");
            }
            sb.append("TRUE");
            return sb.toString();
            */
            throw new IllegalArgumentException("jdbc module does not support DELETE op.");
        } else {
            sb = new StringBuilder("INSERT INTO " + table + " (");
        }
        for(final Schema.Field field : schema.getFields()) {
            sb.append(field.name());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");

        sb.append("VALUES(");
        schema.getFields().forEach(f -> sb.append("?,"));
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");

        if(op.equals(OP.INSERT_OR_UPDATE) || op.equals(OP.INSERT_OR_DONOTHING)) {
            switch (db) {
                case MYSQL: {
                    sb.append(" ON DUPLICATE KEY UPDATE ");
                    if(op.equals(OP.INSERT_OR_DONOTHING)) {
                        for (final String keyField : keyFields) {
                            sb.append("`" + keyField + "`=VALUES(`" + keyField + "`),");
                        }
                    } else {
                        for (final Schema.Field field : schema.getFields()) {
                            if (keyFields.contains(field.name())) {
                                continue;
                            }
                            sb.append("`" + field.name() + "`=VALUES(`" + field.name() + "`),");
                        }
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    break;
                }
                case POSTGRESQL: {
                    sb.append(" ON CONFLICT (");
                    for (final String keyField : keyFields) {
                        sb.append(keyField);
                        sb.append(",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    if(op.equals(OP.INSERT_OR_DONOTHING)) {
                        sb.append(") DO NOTHING");
                    } else {
                        /*
                        sb.append(") DO UPDATE SET ");
                        for (final Schema.Field field : schema.getFields()) {
                            if(keyFields.contains(field.name())) {
                                continue;
                            }
                            sb.append(field.name() + "'" + field.name() + "'),");
                        }
                        sb.deleteCharAt(sb.length() - 1);
                        */
                        throw new IllegalArgumentException("jdbc module does not support PostgreSQL INSERT_OR_UPDATE op.");
                    }
                    break;
                }
                case SQLSERVER: {
                    break;
                }
            }
        }

        return sb.toString();
    }

    public static void setStatement(final PreparedStatement statement,
                                    final int parameterIndex,
                                    final Schema fieldSchema,
                                    final Object fieldValue) throws SQLException {

        final boolean isNull = fieldValue == null;
        switch (fieldSchema.getType()) {
            case BOOLEAN: {
                statement.setBoolean(parameterIndex, (Boolean) fieldValue);
                break;
            }
            case ENUM:
            case STRING: {
                if(isNull) {
                    statement.setString(parameterIndex, null);
                } else {
                    statement.setString(parameterIndex, fieldValue.toString());
                }
                break;
            }
            case FIXED:
            case BYTES: {
                if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                    if(isNull) {
                        statement.setBigDecimal(parameterIndex, null);
                    } else {
                        final ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
                        final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(fieldSchema, byteBuffer);
                        statement.setBigDecimal(parameterIndex, decimal);
                    }
                } else {
                    if(isNull) {
                        statement.setBytes(parameterIndex, null);
                    } else {
                        statement.setBytes(parameterIndex, ((ByteBuffer) fieldValue).array());
                    }
                }
                break;
            }
            case INT: {
                final Integer value = (Integer) fieldValue;
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    if(isNull) {
                        statement.setDate(parameterIndex, null);
                    } else {
                        statement.setDate(parameterIndex, java.sql.Date.valueOf(LocalDate.ofEpochDay(Long.valueOf(value))));
                    }
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    if(isNull) {
                        statement.setTime(parameterIndex, null);
                    } else {
                        statement.setTime(parameterIndex, Time.valueOf(LocalTime.ofNanoOfDay(Long.valueOf(value) * 1000_000L)));
                    }
                } else {
                    try {
                        statement.setInt(parameterIndex, value);

                    }catch (NullPointerException e) {
                        throw new RuntimeException("Statement: " + statement + ", fieldSchema: " + fieldSchema + ", parameterIndex: " + parameterIndex + ", value: " + value, e);
                    }
                }
                break;
            }
            case LONG: {
                final Long value = (Long) fieldValue;
                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    if(isNull) {
                        statement.setTimestamp(parameterIndex, null);
                    } else {
                        statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(DateTimeUtil.toLocalDateTime(value * 1000L)));
                    }
                } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    if(isNull) {
                        statement.setTimestamp(parameterIndex, null);
                    } else {
                        statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(DateTimeUtil.toLocalDateTime(value)));
                    }
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    if(isNull) {
                        statement.setTime(parameterIndex, null);
                    } else {
                        statement.setTime(parameterIndex, Time.valueOf(LocalTime.ofNanoOfDay(Long.valueOf(value) * 1000L)));
                    }
                } else {
                    statement.setLong(parameterIndex, value);
                }
                break;
            }
            case FLOAT: {
                statement.setFloat(parameterIndex, (Float) fieldValue);
                break;
            }
            case DOUBLE: {
                statement.setDouble(parameterIndex, (Double) fieldValue);
                break;
            }
            case NULL: {
                statement.setObject(parameterIndex, null);
                break;
            }
            case UNION:
            case MAP:
            case RECORD:
            case ARRAY:
            default: {
                throw new IllegalStateException("Not supported prepare parameter type: " + fieldSchema.getType().getName());
            }
        }
    }

    public static DB extractDbFromDriver(final String driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver must not be null");
        }
        if(driver.contains("mysql")) {
            return DB.MYSQL;
        } else if(driver.contains("postgresql")) {
            return DB.POSTGRESQL;
        } else if(driver.contains("sqlserver")) {
            return DB.SQLSERVER;
        } else {
            throw new IllegalArgumentException("Not supported database: " + driver);
        }
    }

    private static boolean isValidColumnType(final Schema fieldSchema) {
        switch (fieldSchema.getType()) {
            case MAP:
            case RECORD:
                return false;
            case ARRAY:
                if(!isValidColumnType(fieldSchema.getElementType())) {
                    return false;
                }
                return true;
            case UNION:
                return isValidColumnType(AvroSchemaUtil.unnestUnion(fieldSchema));
            default:
                return true;
        }
    }

    private static String getColumnType(final Schema schema, final DB db) {
        final Schema avroSchema = AvroSchemaUtil.unnestUnion(schema);
        switch (avroSchema.getType()) {
            case BOOLEAN: {
                switch (db) {
                    case MYSQL:
                        return "TINYINT(1)";
                    case POSTGRESQL:
                        return "BOOLEAN";
                    default:
                        return "BOOLEAN";
                }
            }
            case ENUM:
            case STRING: {
                switch (db) {
                    case MYSQL:
                        return "TEXT CHARACTER SET utf8mb4";
                    case POSTGRESQL:
                        return "TEXT";
                    default:
                        return "TEXT";
                }
            }
            case FIXED:
            case BYTES: {
                if (AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    return "DECIMAL(38, 9)";
                }
                switch (db) {
                    case MYSQL:
                        return "MEDIUMBLOB";
                    case POSTGRESQL:
                        return "BYTEA";
                    default:
                        break;
                }
                return "BLOB";
            }
            case INT:
                if (LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    return "DATE";
                } else if (LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    return "TIME";
                } else {
                    return "INTEGER";
                }
            case LONG: {
                switch (db) {
                    case MYSQL: {
                        if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                            return "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                        } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                            return "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                        } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                            return "TIME";
                        } else {
                            return "BIGINT";
                        }
                    }
                    case POSTGRESQL: {
                        if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                            return "TIMESTAMP";
                        } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                            return "TIMESTAMP";
                        } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                            return "TIME";
                        } else {
                            return "BIGINT";
                        }
                    }
                    case SQLSERVER: {
                        if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                            return "TIMESTAMP";
                        } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                            return "TIMESTAMP";
                        } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                            return "TIME";
                        } else {
                            return "BIGINT";
                        }
                    }
                }
            }
            case FLOAT:
                return "REAL";
            case DOUBLE: {
                switch (db) {
                    case MYSQL:
                        return "DOUBLE";
                    case POSTGRESQL:
                        return "DOUBLE PRECISION";
                    default:
                        break;
                }
                return "DOUBLE";
            }
            case ARRAY: {

            }
            case NULL:
            case MAP:
            case RECORD:
            case UNION:
            default:
                throw new IllegalArgumentException(String.format("DataType: %s is not supported!", avroSchema.getType().name()));

        }
    }

    private static String replaceReservedKeyword(final String term) {
        if(RESERVED_KEYWORDS.contains(term.trim().toUpperCase())) {
            return String.format("`%s`", term);
        }
        return term;
    }

    public static List<IndexRange> splitIndexRange(
            final List<IndexOffset> parents,
            final List<IndexOffset> from,
            final List<IndexOffset> to,
            final int splitNum) {

        final IndexOffset firstFromOffset = from.get(0);
        final IndexOffset firstToOffset = to.get(0);

        final List<IndexOffset> splitOffsets;
        switch (firstFromOffset.getFieldType()) {
            case BOOLEAN:
                splitOffsets = splitBoolean(firstFromOffset.getFieldName(), firstToOffset.getAscending());
                break;
            case INT:
                splitOffsets = splitInteger(firstFromOffset.getFieldName(),
                        firstFromOffset.getIntValue(), firstToOffset.getIntValue(),
                        firstToOffset.getAscending(), splitNum);
                break;
            case LONG:
                splitOffsets = splitLong(firstFromOffset.getFieldName(),
                        firstFromOffset.getLongValue(), firstToOffset.getLongValue(),
                        firstToOffset.getAscending(), splitNum);
                break;
            case FLOAT:
                splitOffsets = splitFloat(firstFromOffset.getFieldName(),
                        firstFromOffset.getFloatValue(), firstToOffset.getFloatValue(),
                        firstToOffset.getAscending(), splitNum);
                break;
            case DOUBLE:
                splitOffsets = splitDouble(firstFromOffset.getFieldName(),
                        firstFromOffset.getDoubleValue(), firstToOffset.getDoubleValue(),
                        firstToOffset.getAscending(), splitNum);
                break;
            case ENUM:
            case STRING: {
                splitOffsets = splitString(firstFromOffset.getFieldName(),
                        firstFromOffset.getStringValue(), firstToOffset.getStringValue(),
                        firstToOffset.getAscending(), splitNum);
                break;
            }
            case FIXED:
            case BYTES:
            default: {
                throw new IllegalArgumentException();
            }
        }

        List<IndexOffset> parentOffsets = new ArrayList<>();
        if(parents != null && parents.size() > 0) {
            parentOffsets.addAll(parents);
        }

        if(splitOffsets.size() == 0) {
            if(from.size() > 1 && to.size() > 1 && false) {
                return splitIndexRange(parentOffsets, from.subList(1, from.size()), to.subList(1, to.size()), splitNum);
            } else {
                return Arrays.asList(IndexRange.of(
                        IndexPosition.of(from, true),
                        IndexPosition.of(to, false)));
            }
        } else {
            final List<IndexRange> results = new ArrayList<>();
            List<IndexOffset> nextFrom = new ArrayList<>(parentOffsets);
            nextFrom.addAll(from);
            for(final IndexOffset offset : splitOffsets) {
                final List<IndexOffset> nextTo = new ArrayList<>(parentOffsets);
                nextTo.add(offset);
                final IndexRange range = IndexRange.of(
                        IndexPosition.of(nextFrom, true),
                        IndexPosition.of(nextTo, false));
                results.add(range);
                List<IndexOffset> offsets = new ArrayList<>(parentOffsets);
                offsets.add(offset);
                nextFrom = offsets;
            }

            return results;
        }
    }

    private static List<IndexOffset> splitBoolean(final String name, final boolean ascending) {
        final List<IndexOffset> results = new ArrayList<>();
        results.add(IndexOffset.of(name, Schema.Type.BOOLEAN, ascending, Boolean.FALSE));
        results.add(IndexOffset.of(name, Schema.Type.BOOLEAN, ascending, Boolean.TRUE));
        return results;
    }

    private static List<IndexOffset> splitInteger(final String name, Integer min, Integer max, final boolean ascending, final int splitNum) {
        if(min == null) {
            min = 0;
        }
        if (max == null) {
            max = Integer.MAX_VALUE;
        }
        if(min == max) {
            return new ArrayList<>();
        }
        final List<IndexOffset> results = new ArrayList<>();
        final double boundSize = (double)(max - min) / splitNum;
        int prev = min;
        for(int i=1; i<splitNum; i++) {
            int next = (int)Math.round(min + boundSize * i);
            if(prev == next || next > max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, next));
        }
        results.add(IndexOffset.of(name, Schema.Type.INT, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitLong(final String name, Long min, Long max, final boolean ascending, final int splitNum) {
        if(min == null) {
            min = 0L;
        }
        if (max == null) {
            max = Long.MAX_VALUE;
        }
        final List<IndexOffset> results = new ArrayList<>();
        final double boundSize = (double)((max - min) / splitNum);
        long prev = min;
        for(int i=1; i<splitNum; i++) {
            long next = Math.round(min + boundSize * i);
            if(prev == next || next > max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, next));
        }
        results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitFloat(final String name, Float min, Float max, final boolean ascending, final int splitNum) {
        if(min == null) {
            min = 0F;
        }
        if (max == null) {
            max = Float.MAX_VALUE;
        }
        final List<IndexOffset> results = new ArrayList<>();
        final float boundSize = (max - min) / splitNum;
        float prev = min;
        for(int i=1; i<splitNum; i++) {
            float next = min + boundSize * i;
            if(prev == next || next > max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, next));
        }
        results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitDouble(final String name, Double min, Double max, final boolean ascending, final int splitNum) {
        if(min == null) {
            min = 0D;
        }
        if (max == null) {
            max = Double.MAX_VALUE;
        }
        final List<IndexOffset> results = new ArrayList<>();
        final double boundSize = (max - min) / splitNum;
        double prev = min;
        for(int i=1; i<splitNum; i++) {
            double next = (min + boundSize * i);
            if(prev == next || next > max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, next));
        }
        results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, max));
        return results;
    }

    public static List<IndexOffset> splitString(final String name, String min, String max, final boolean ascending, final int splitNum) {
        if(min == null || min.length() == 0) {
            final StringBuilder sb = new StringBuilder();
            sb.append(String.valueOf((char) 33).repeat(32));
            min = sb.toString();
        }
        if(max == null || max.length() == 0) {
            final StringBuilder sb = new StringBuilder();
            sb.append(String.valueOf((char) 126).repeat(32));
            max = sb.toString();
        }

        final char[] mins = min.toCharArray();
        final char[] maxs = max.toCharArray();
        final List<String> strs = splitChar(mins, maxs, 0, splitNum);
        return strs.stream()
                .map(s -> IndexOffset.of(name, Schema.Type.STRING, ascending, s))
                .collect(Collectors.toList());
    }

    private static List<IndexOffset> splitNumeric(final String name, BigDecimal min, BigDecimal max, final boolean ascending, final int splitNum) {
        if(min == null) {
            min = BigDecimal.ZERO;
        }
        if (max == null) {
            max = BigDecimal.valueOf(Double.MAX_VALUE);
        }
        final List<IndexOffset> results = new ArrayList<>();
        final BigDecimal boundSize = (max.subtract(min)).divide(BigDecimal.valueOf(splitNum));
        BigDecimal prev = min;
        for(int i=1; i<splitNum; i++) {
            BigDecimal next = min.add((boundSize.multiply(BigDecimal.valueOf(i))));
            if(prev == next || next.subtract(max).doubleValue() > 0D) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, next));
        }
        results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, max));
        return results;
    }

    private static List<String> splitChar(char[] min, char[] max, int index, int splitNum) {
        if(index >= min.length || index >= max.length) {
            return new ArrayList<>();
        }
        final char cmin = min[index];
        final char cmax = max[index];
        final int diff = cmax - cmin;
        if(diff < 0) {
            throw new IllegalStateException("Illegal string min: " + min + ", max: " + max);
        } else if(diff == 0) {
            return splitChar(
                    min,
                    max,
                    index + 1,
                    splitNum);
        } else {
            final List<Character> results = new ArrayList<>();
            final double boundSize = (double)diff / (double)splitNum;
            char prev = cmin;
            for(int i=1; i<splitNum; i++) {
                char next = (char)Math.round(cmin + boundSize * i);
                if(prev == next || next > cmax) {
                    continue;
                }
                results.add(next);
            }
            results.add(cmax);

            char[] prefix = new char[index + 1];
            for(int i=0; i<index; i++) {
                prefix[i] = min[i];
            }

            final List<String> strs = new ArrayList<>();
            for(int i=0; i<results.size() - 1; i++) {
                prefix[index] = results.get(i);
                strs.add(String.valueOf(prefix));
            }
            strs.add(String.valueOf(max));
            return strs;
        }
    }

    public static String createSeekPreparedQuery(
            final IndexPosition startPosition,
            final IndexPosition stopPosition,
            final String fields,
            final String table,
            final List<String> parameterFields,
            final Integer limit) {

        String preparedQuery = String.format("SELECT %s FROM %s", fields, table);

        final List<String> startConditions = createSeekConditions(startPosition.getOffsets(), true, startPosition.getIsOpen());
        final List<String> stopConditions = createSeekConditions(stopPosition.getOffsets(), false, stopPosition.getIsOpen());

        final String startCondition = "(" + String.join(" OR ", startConditions) + ")";
        final String stopCondition = "(" + String.join(" OR ", stopConditions) + ")";

        final String condition = startCondition + " AND " + stopCondition;
        preparedQuery = preparedQuery + " WHERE " + condition;

        final String parameterFieldsString = parameterFields.stream()
                .map(f -> f.replaceFirst(":", " "))
                .collect(Collectors.joining(", "));
        preparedQuery = preparedQuery + String.format(" ORDER BY %s LIMIT %d", parameterFieldsString, limit);
        return preparedQuery;
    }

    public static List<String> createSeekConditions(
            final List<IndexOffset> offsets,
            final boolean isStart,
            final boolean isOpen) {

        if(offsets.size() == 1) {
            final IndexOffset offset = offsets.get(0);
            final String operation = (isStart ? Condition.GREATER : Condition.LESSER).getName(offset.getAscending()) + (isOpen ? "" : "=");
            final String condition = offset.getFieldName() + " " + operation + " ?";
            return Arrays.asList(condition);
        }
        final List<String> andConditions = new ArrayList<>();
        for(int i=0; i<offsets.size()-1; i++) {
            final IndexOffset offset = offsets.get(i);
            final String andCondition = offset.getFieldName() + " = ?";
            andConditions.add(andCondition);
        }
        final IndexOffset offset = offsets.get(offsets.size() - 1);
        final String operation = (isStart ? Condition.GREATER : Condition.LESSER).getName(offset.getAscending()) + (isOpen ? "" : "=");
        final String andCondition = offset.getFieldName() + " " + operation + " ?";
        andConditions.add(andCondition);

        final String condition = "(" + String.join(" AND ", andConditions) + ")";
        final List<String> conditions = new ArrayList<>();
        conditions.add(condition);

        final List<String> childrenConditions = createSeekConditions(offsets.subList(0, offsets.size() - 1), isStart, isOpen);
        conditions.addAll(childrenConditions);
        return conditions;
    }

    public static int setStatementParameters(
            final PreparedStatement statement,
            final List<JdbcUtil.IndexOffset> offsets,
            final Map<String, Schema.Field> fields,
            final int paramIndexOffset) throws SQLException {

        int paramIndex = paramIndexOffset;
        for(IndexOffset offset : offsets) {
            final Object value = offset.getValue();
            Schema.Field field = fields.get(offset.getFieldName());
            if(field == null) {
                // For PostgreSQL
                field = fields.get(offset.getFieldName().toLowerCase());
            }
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            JdbcUtil.setStatement(statement, paramIndex, fieldSchema, value);
            paramIndex = paramIndex + 1;
        }
        if(offsets.size() > 0) {
            paramIndex = setStatementParameters(
                    statement,
                    offsets.subList(0, offsets.size() - 1),
                    fields,
                    paramIndex);
        }
        return paramIndex;
    }

    public enum Condition implements Serializable {
        GREATER(">"),
        LESSER("<"),
        EQUAL("=");

        private final String name;

        public String getName() {
            return this.name;
        }

        public String getName(final boolean ascending) {
            if(ascending) {
                return this.getName();
            } else {
                return this.reverse().getName();
            }
        }

        Condition(String name) {
            this.name = name;
        }

        public Condition reverse() {
            if(GREATER.equals(this)) {
                return LESSER;
            } else if(LESSER.equals(this)) {
                return GREATER;
            } else {
                return this;
            }
        }
    }

    @DefaultCoder(AvroCoder.class)
    public static class IndexOffset {

        @Nullable
        private String fieldName;
        @Nullable
        private Schema.Type fieldType;
        @Nullable
        private Boolean ascending;

        @Nullable
        private Boolean booleanValue;
        @Nullable
        private String stringValue;
        @Nullable
        private ByteBuffer bytesValue;
        @Nullable
        private Integer intValue;
        @Nullable
        private Long longValue;
        @Nullable
        private Float floatValue;
        @Nullable
        private Double doubleValue;


        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public Schema.Type getFieldType() {
            return fieldType;
        }

        public void setFieldType(Schema.Type fieldType) {
            this.fieldType = fieldType;
        }

        public Boolean getAscending() {
            return ascending;
        }

        public void setAscending(Boolean ascending) {
            this.ascending = ascending;
        }

        public Boolean getBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(Boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        public void setStringValue(String stringValue) {
            this.stringValue = stringValue;
        }

        public ByteBuffer getBytesValue() {
            return bytesValue;
        }

        public void setBytesValue(ByteBuffer bytesValue) {
            this.bytesValue = bytesValue;
        }

        public Integer getIntValue() {
            return intValue;
        }

        public void setIntValue(Integer intValue) {
            this.intValue = intValue;
        }

        public Long getLongValue() {
            return longValue;
        }

        public void setLongValue(Long longValue) {
            this.longValue = longValue;
        }

        public Float getFloatValue() {
            return floatValue;
        }

        public void setFloatValue(Float floatValue) {
            this.floatValue = floatValue;
        }

        public Double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(Double doubleValue) {
            this.doubleValue = doubleValue;
        }

        public Object getValue() {
            switch (this.fieldType) {
                case BOOLEAN: {
                    return this.booleanValue;
                }
                case ENUM:
                case STRING: {
                    return this.stringValue;
                }
                case FIXED:
                case BYTES: {
                    return this.bytesValue;
                }
                case INT: {
                    return this.intValue;
                }
                case LONG: {
                    return this.longValue;
                }
                case FLOAT: {
                    return this.floatValue;
                }
                case DOUBLE: {
                    return this.doubleValue;
                }
                case NULL:
                    return null;
                case UNION:
                case MAP:
                case RECORD:
                case ARRAY:
                default: {
                    throw new IllegalArgumentException("Not supported range type: " + fieldType);
                }
            }
        }

        public boolean isGreaterThan(final IndexOffset another) {
            return compareTo(another) > 0;
        }

        public boolean isLesserThan(final IndexOffset another) {
            return compareTo(another) < 0;
        }

        public int compareTo(final IndexOffset another) {
            if(this.getValue() == null && another.getValue() == null) {
                return 0;
            } else if(this.getValue() == null) {
                return -1;
            } else if(another.getValue() == null) {
                return 1;
            }
            switch (this.fieldType) {
                case BOOLEAN: {
                    return this.booleanValue.compareTo(another.getBooleanValue());
                }
                case ENUM:
                case STRING: {
                    return this.stringValue.compareTo(another.getStringValue());
                }
                case FIXED:
                case BYTES: {
                    return this.bytesValue.compareTo(another.getBytesValue());
                }
                case INT: {
                    return this.intValue.compareTo(another.getIntValue());
                }
                case LONG: {
                    return this.longValue.compareTo(another.getLongValue());
                }
                case FLOAT: {
                    return this.floatValue.compareTo(another.getFloatValue());
                }
                case DOUBLE: {
                    return this.doubleValue.compareTo(another.getDoubleValue());
                }
                case NULL:
                    return 0;
                case UNION:
                case MAP:
                case RECORD:
                case ARRAY:
                default: {
                    throw new IllegalArgumentException("Not supported range type: " + fieldType);
                }
            }
        }

        @Override
        public String toString() {
            return String.format("IndexOffset: %s = %s",
                    this.getFieldName() + (this.getAscending() ? "" : "(DESC)"),
                    this.getValue());
        }

        public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value) {
            final IndexOffset indexOffset = new IndexOffset();
            indexOffset.setFieldName(fieldName);
            indexOffset.setFieldType(fieldType);
            indexOffset.setAscending(ascending);
            switch (fieldType) {
                case BOOLEAN: {
                    indexOffset.booleanValue = (Boolean) value;
                    break;
                }
                case ENUM:
                case STRING: {
                    if(value == null) {
                        indexOffset.stringValue = null;
                    } else {
                        indexOffset.stringValue = value.toString();
                    }
                    break;
                }
                case FIXED:
                case BYTES: {
                    indexOffset.bytesValue = (ByteBuffer) value;
                    break;
                }
                case INT: {
                    if(value == null) {
                        indexOffset.intValue = null;
                    } else if(value instanceof Long) {
                        indexOffset.intValue = ((Long) value).intValue();
                    } else {
                        indexOffset.intValue = (Integer) value;
                    }
                    break;
                }
                case LONG: {
                    indexOffset.longValue = (Long) value;
                    break;
                }
                case FLOAT: {
                    indexOffset.floatValue = (Float) value;
                    break;
                }
                case DOUBLE: {
                    indexOffset.doubleValue = (Double) value;
                    break;
                }
                case NULL:
                case UNION:
                    break;
                case MAP:
                case RECORD:
                case ARRAY:
                default: {
                    throw new IllegalArgumentException("Not supported range type: " + fieldType);
                }
            }
            return indexOffset;
        }
    }

    @DefaultCoder(AvroCoder.class)
    public static class IndexPosition {

        @Nullable
        private Boolean completed;
        @Nullable
        private Long count;

        @Nullable
        private Boolean isOpen;
        @Nullable
        private List<IndexOffset> offsets;

        public Boolean getCompleted() {
            return completed;
        }

        public void setCompleted(Boolean completed) {
            this.completed = completed;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Boolean getIsOpen() {
            return isOpen;
        }

        public void setIsOpen(Boolean isOpen) {
            this.isOpen = isOpen;
        }

        public List<IndexOffset> getOffsets() {
            return offsets;
        }

        public void setOffsets(List<IndexOffset> offsets) {
            this.offsets = offsets;
        }

        public boolean isOverTo(final IndexPosition another) {
            final int size = Math.min(this.getOffsets().size(), another.getOffsets().size());
            for(int i=0; i<size; i++) {
                final JdbcUtil.IndexOffset bound = another.getOffsets().get(i);
                if(bound.getValue() == null) {
                    return false;
                }
                final JdbcUtil.IndexOffset point = this.getOffsets().get(i);
                if(point.isLesserThan(bound)) {
                    return false;
                } else if(point.isGreaterThan(bound)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            final String o = this.offsets.stream()
                    .map(offset -> String.format("%s = %s",
                            offset.getFieldName() + (offset.getAscending() ? "" : "(DESC)"),
                            offset.getValue()))
                    .collect(Collectors.joining(", "));

            return o + " [open=" + isOpen + "]";
        }

        public static IndexPosition of(final List<IndexOffset> offsets, final boolean isOpen) {
            if(offsets == null || offsets.size() == 0) {
                throw new IllegalArgumentException("offsets must not be null or zero size for IndexPosition");
            }
            final IndexPosition indexPosition = new IndexPosition();
            indexPosition.setCount(0L);
            indexPosition.setCompleted(false);
            indexPosition.setIsOpen(isOpen);
            indexPosition.setOffsets(offsets);
            return indexPosition;
        }

    }

    @DefaultCoder(AvroCoder.class)
    public static class IndexRange {

        @Nullable
        private Long totalSize;
        @Nullable
        private Double ratio;

        @Nullable
        private IndexPosition from;
        @Nullable
        private IndexPosition to;

        public IndexRange() {

        }

        public Double getRatio() {
            return ratio;
        }

        public void setRatio(Double ratio) {
            this.ratio = ratio;
        }

        public IndexPosition getFrom() {
            return from;
        }

        public void setFrom(IndexPosition from) {
            this.from = from;
        }

        public IndexPosition getTo() {
            return to;
        }

        public void setTo(IndexPosition to) {
            this.to = to;
        }

        @Override
        public String toString() {
            return String.format("IndexRange(%f) From: %s -> To: %s%s",
                    this.ratio,
                    from.toString(),
                    to.toString(),
                    from.getCompleted() ? "  completed count: " + from.getCount() : "");
        }

        public static IndexRange of(IndexPosition from, IndexPosition to) {
            if(from == null || to == null) {
                throw new IllegalArgumentException("Both from and to must not be null for IndexRange");
            }
            if(from.getOffsets() == null || to.getOffsets() == null) {
                throw new IllegalArgumentException("Both from and to must not be null for IndexRange");
            }
            final IndexRange indexRange = new IndexRange();
            indexRange.setRatio(1.0D);
            indexRange.setFrom(from);
            indexRange.setTo(to);
            return indexRange;
        }

    }

}
