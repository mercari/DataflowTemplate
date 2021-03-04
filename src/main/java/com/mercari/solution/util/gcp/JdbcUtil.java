package com.mercari.solution.util.gcp;

import com.mercari.solution.util.schema.AvroSchemaUtil;
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
            final String driverClassName, final String url,
            final String username, final String password) {

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
