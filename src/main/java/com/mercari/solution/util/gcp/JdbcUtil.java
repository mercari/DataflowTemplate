package com.mercari.solution.util.gcp;

import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.converter.ResultSetToRecordConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcUtil {

    private static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            "ALL","AND","ANY","ARRAY","AS","ASC","ASSERT_ROWS_MODIFIED","AT",
            "BETWEEN","BY","CASE","CAST","COLLATE","CONTAINS","CREATE","CROSS","CUBE","CURRENT",
            "DEFAULT","DEFINE","DESC","DISTINCT","ELSE","END","ENUM","ESCAPE","EXCEPT","EXCLUDE","EXISTS","EXTRACT",
            "FALSE","FETCH","FOLLOWING","FOR","FROM","FULL","GROUP","GROUPING","GROUPS","HASH","HAVING",
            "IF","IGNORE","IN","INNER","INTERSECT","INTERVAL","INTO","IS","JOIN",
            "LATERAL","LEFT","LIKE","LIMIT","LOOKUP","MERGE","NATURAL","NEW","NO","NOT","NULL","NULLS",
            "OF","ON","OR","ORDER","OUTER","OVER","PARTITION","PRECEDING","PROTO","RANGE");

    public static DataSource createDataSource(
            final String driverClassName, final String url,
            final String username, final String password) throws Exception {

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
        poolableConnectionFactory.setDefaultReadOnly(false);
        return new PoolingDataSource(connectionPool);

    }

    public static Schema createAvroSchema(
            final String driverClassName, final String url,
            final String username, final String password,
            final String query) throws Exception {

        try(final BasicDataSource basicDataSource = new BasicDataSource()) {

            basicDataSource.setDriverClassName(driverClassName);
            basicDataSource.setUrl(url);
            basicDataSource.setUsername(username);
            basicDataSource.setPassword(password);

            final DataSourceConnectionFactory connectionFactory = new DataSourceConnectionFactory(basicDataSource);
            final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
            final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            poolConfig.setMaxTotal(1);
            poolConfig.setMinIdle(0);
            poolConfig.setMinEvictableIdleTimeMillis(10000);
            poolConfig.setSoftMinEvictableIdleTimeMillis(30000);

            try(final GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory, poolConfig)) {
                poolableConnectionFactory.setPool(connectionPool);
                poolableConnectionFactory.setDefaultAutoCommit(false);
                poolableConnectionFactory.setDefaultReadOnly(false);

                try (final PoolingDataSource source = new PoolingDataSource(connectionPool);
                     final Connection connection = source.getConnection();
                     final PreparedStatement statement = connection
                             .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                    statement.setFetchSize(1);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if(resultSet.next()) {
                            return ResultSetToRecordConverter.convertSchema(resultSet);
                        } else {
                            return ResultSetToRecordConverter.convertSchema(resultSet);
                        }
                    }
                }
            }

        }
    }

    public static boolean existsTable(final String table) {
        return false;
    }

    public static String buildCreateTableSQL(final Schema schema,
                                             final String table,
                                             final List<String> keyFields) {

        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s (", table));
        schema.getFields().stream()
                .filter(f -> isValidColumnType(f.schema()))
                .forEach(f -> sb.append(String.format("%s %s%s,",
                        replaceReservedKeyword(f.name()),
                        getColumnType(f.schema()),
                        AvroSchemaUtil.isNullable(f.schema()) ? "" : " NOT NULL")));
        sb.deleteCharAt(sb.length() - 1);
        if(keyFields == null || keyFields.size() == 0) {
            sb.append(");");
        } else {
            final String primaryKey = keyFields.stream()
                    .map(JdbcUtil::replaceReservedKeyword)
                    .collect(Collectors.joining(","));
            sb.append(String.format(") PRIMARY KEY ( %s );", primaryKey));
        }
        return sb.toString();
    }

    private static boolean isValidColumnType(final Schema fieldType) {
        switch (fieldType.getType()) {
            case MAP:
            case RECORD:
                return false;
            case ARRAY:
                if(!isValidColumnType(fieldType.getElementType())) {
                    return false;
                }
                return true;
            default:
                return true;
        }
    }

    private static String getColumnType(final Schema schema) {
        final Schema avroSchema = AvroSchemaUtil.unnestUnion(schema);
        switch (avroSchema.getType()) {
            case BOOLEAN:
                return "TINYINT(1)";
            case ENUM:
            case STRING:
                return "TEXT CHARACTER SET utf8";
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    return "DECIMAL(38, 9)";
                }
                return "BLOB(65535)";
            case INT:
                if (LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    return "DATE";
                } else if (LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    return "TIME";
                } else {
                    return "INTEGER";
                }
            case LONG:
                if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                    return "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                    return "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                    return "TIME";
                } else {
                    return "BIGINT";
                }
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
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


}
