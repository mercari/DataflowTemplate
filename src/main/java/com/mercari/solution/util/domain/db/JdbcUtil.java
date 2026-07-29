package com.mercari.solution.util.domain.db;

import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import com.mercari.solution.util.domain.db.stmt.PreparedStatementTemplate;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class JdbcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    public enum DB {
        MYSQL,
        POSTGRESQL,
        SQLSERVER,
        H2
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


    public static CloseableDataSource createDataSource(
            final String driverClassName,
            final String url,
            final String username,
            final String password) {

        return createDataSource(driverClassName, url, username, password, false);
    }

    public static CloseableDataSource createDataSource(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final boolean readOnly) {

        final HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverClassName);
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setReadOnly(readOnly);
        config.setMaximumPoolSize(1);
        config.setAutoCommit(false);
        final DataSource dataSource = new HikariDataSource(config);
        return new CloseableDataSource(dataSource);
    }

    public static Schema createAvroSchemaFromTable(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final String table) throws SQLException {

        try {
            Class.forName(driverClassName);
            try(final Connection connection = DriverManager.getConnection(url, username, password)) {
                return createAvroSchemaFromTable(connection, table);
            }
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("driver class: " + driverClassName + " not found", e);
        }
    }

    public static Schema createAvroSchemaFromTable(
        final Connection connection,
        final String table) throws SQLException {

        final String tableQuery = String.format("SELECT * FROM %s LIMIT 1", table);
        try(final PreparedStatement statement = connection.prepareStatement(tableQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            if(statement.getMetaData() == null) {
                throw new IllegalArgumentException("Failed to get schema for query: " + tableQuery);
            }
            final Schema schema = ResultSetToRecordConverter.convertSchema(statement.getMetaData());
            final List<String> parameterFieldNames = getPrimaryKeyNames(connection, null, null, table);
            final String parameterFieldNamesAttr = String.join(",", parameterFieldNames);
            final SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder
                    .record("root")
                    .prop("table", table)
                    .prop("primaryKeys", parameterFieldNamesAttr)
                    .fields();
            schema.getFields().forEach(f -> schemaBuilder.name(f.name()).type(f.schema()).noDefault());
            return schemaBuilder.endRecord();
        }
    }

    public static Schema createAvroSchemaFromQuery(
            final String driverClassName,
            final String url,
            final String username,
            final String password,
            final String query,
            final List<String> prepareCalls) throws Exception {

        try {
            Class.forName(driverClassName);
            try(final Connection connection = DriverManager.getConnection(url, username, password)) {
                return createAvroSchemaFromQuery(connection, query, prepareCalls);
            }
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("driver class: " + driverClassName + " not found", e);
        }
    }

    public static Schema createAvroSchemaFromQuery(
            final Connection connection,
            final String query,
            final List<String> prepareCalls) throws SQLException {

        for(final String prepareCall : Optional.ofNullable(prepareCalls).orElseGet(List::of)) {
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
                        getColumnType(f.schema(), db, keyFields.contains(f.name())),
                        AvroSchemaUtil.isNullable(f.schema()) ? "" : " NOT NULL")));

        if(keyFields == null || keyFields.isEmpty()) {
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

    public static PreparedStatementTemplate createStatement(final String table, final Schema schema,
                                         final OP op, final DB db,
                                         final List<String> keyFields) {
        return createStatement(table, schema, op, db, keyFields, 1);
    }

    public static PreparedStatementTemplate createStatement(final String table, final Schema schema,
                                         final OP op, final DB db,
                                         final List<String> keyFields,
                                         final int bulkInsertSize) {

        if(op.equals(OP.DELETE)) {
            throw new IllegalArgumentException("jdbc module does not support DELETE op.");
        }
        if((op.equals(OP.INSERT_OR_UPDATE) || op.equals(OP.INSERT_OR_DONOTHING))
                && (keyFields == null || keyFields.isEmpty())) {
            throw new IllegalArgumentException("keyFields must not be empty for op: " + op);
        }
        if(bulkInsertSize < 1) {
            throw new IllegalArgumentException("bulkInsertSize must be greater than or equal to 1.");
        }

        return switch (db) {
            case MYSQL -> createMySQLStatement(table, schema, op, keyFields, bulkInsertSize);
            case POSTGRESQL -> createPostgreSQLStatement(table, schema, op, keyFields, bulkInsertSize);
            case H2 -> createH2Statement(table, schema, op, keyFields, bulkInsertSize);
            case SQLSERVER -> createSQLServerStatement(table, schema, op, keyFields, bulkInsertSize);
            default -> throw new IllegalArgumentException("Not supported database: " + db);
        };
    }

    private static PreparedStatementTemplate createMySQLStatement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields,
                                         final int bulkInsertSize) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        sb.appendString("INSERT INTO ").appendString(table);

        sb.appendString(" (");
        schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
        sb.removeLast();
        sb.appendString(")");

        sb.appendString(" VALUES ");
        IntStream.range(0, bulkInsertSize).forEach(rowIndex -> {
            sb.appendString("(");
            IntStream.range(0, schema.getFields().size()).forEach(
                    i -> sb.appendPlaceholder(rowIndex * schema.getFields().size() + i + 1).appendString(",")
            );
            sb.removeLast();
            sb.appendString(")").appendString(",");
        });
        sb.removeLast();

        if(op.equals(OP.INSERT_OR_UPDATE)) {
            sb.appendString(" ON DUPLICATE KEY UPDATE ");
            schema.getFields().forEach(f -> {
                if (keyFields.contains(f.name())) return;

                sb.appendBackQuoted(f.name()).appendString(" = VALUES(").appendBackQuoted(f.name()).appendString(")");
                sb.appendString(",");
            });
            sb.removeLast();
        } else if(op.equals(OP.INSERT_OR_DONOTHING)) {
            sb.appendString(" ON DUPLICATE KEY UPDATE ");
            keyFields.forEach(f -> {
                sb.appendBackQuoted(f).appendString(" = VALUES(").appendBackQuoted(f).appendString(")");
                sb.appendString(",");
            });
            sb.removeLast();
        }

        return sb.build();
    }

    private static void appendPostgreSQLTypedPlaceholder(PreparedStatementTemplate.Builder sb, int index , Schema.Field field) {
        sb.appendPlaceholder(index);

        Schema normalizedSchema;
        if (field.schema().getType() == Schema.Type.UNION) {
            normalizedSchema = AvroSchemaUtil.unnestUnion(field.schema());
        } else {
            normalizedSchema = field.schema();
        }

        LogicalType logicalType = normalizedSchema.getLogicalType();
        if (LogicalTypes.date().equals(logicalType)) {
            sb.appendString("::date");
        } else if (LogicalTypes.timestampMicros().equals(logicalType) || LogicalTypes.timestampMillis().equals(logicalType)) {
            sb.appendString("::timestamp");
        }
    }

    private static PreparedStatementTemplate createPostgreSQLStatement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields,
                                         final int bulkInsertSize) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        if (op.equals(OP.INSERT)) {
            sb.appendString("INSERT INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES ");
            IntStream.range(0, bulkInsertSize).forEach(rowIndex -> {
                sb.appendString("(");
                IntStream.range(0, schema.getFields().size()).forEach(i -> {
                    appendPostgreSQLTypedPlaceholder(
                            sb, rowIndex * schema.getFields().size() + i + 1, schema.getFields().get(i));
                    sb.appendString(",");
                });
                sb.removeLast();
                sb.appendString(")").appendString(",");
            });
            sb.removeLast();
        } else if (op.equals(OP.INSERT_OR_UPDATE) || op.equals(OP.INSERT_OR_DONOTHING)) {
            sb.appendString("MERGE INTO ");
            sb.appendString(table);

            sb.appendString(" USING (VALUES ");
            IntStream.range(0, bulkInsertSize).forEach(rowIndex -> {
                sb.appendString("(");
                IntStream.range(0, schema.getFields().size()).forEach(i -> {
                    appendPostgreSQLTypedPlaceholder(
                            sb, rowIndex * schema.getFields().size() + i + 1, schema.getFields().get(i));
                    sb.appendString(",");
                });
                sb.removeLast();
                sb.appendString(")").appendString(",");
            });
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" AS item (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(") ON ");

            keyFields.forEach(f -> {
                sb.appendString("item.").appendString(f).appendString(" = ").appendString(table).appendString(".").appendString(f);
                sb.appendString(" AND ");
            });
            sb.removeLast();

            sb.appendString(" WHEN MATCHED THEN ");

            if (op.equals(OP.INSERT_OR_DONOTHING)) {
                sb.appendString("DO NOTHING");
            } else {
                sb.appendString("UPDATE SET ");
                schema.getFields().forEach(f -> {
                    if (keyFields.contains(f.name())) return;

                    sb.appendString(f.name()).appendString(" = item.").appendString(f.name());
                    sb.appendString(",");
                });
                sb.removeLast();
            }

            sb.appendString(" WHEN NOT MATCHED THEN ");

            sb.appendString("INSERT (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(") VALUES (");
            schema.getFields().forEach(f -> sb.appendString("item.").appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");
        }

        return sb.build();
    }

    private static PreparedStatementTemplate createH2Statement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields,
                                         final int bulkInsertSize) {

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        if(op.equals(OP.INSERT)) {
            sb.appendString("INSERT INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES ");
            IntStream.range(0, bulkInsertSize).forEach(rowIndex -> {
                sb.appendString("(");
                IntStream.range(0, schema.getFields().size()).forEach(
                        i -> sb.appendPlaceholder(rowIndex * schema.getFields().size() + i + 1).appendString(",")
                );
                sb.removeLast();
                sb.appendString(")").appendString(",");
            });
            sb.removeLast();
        } else if(op.equals(OP.INSERT_OR_UPDATE)) {
            sb.appendString("MERGE INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" KEY (");
            keyFields.forEach(f -> sb.appendString(f).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES ");
            IntStream.range(0, bulkInsertSize).forEach(rowIndex -> {
                sb.appendString("(");
                IntStream.range(0, schema.getFields().size()).forEach(
                        i -> sb.appendPlaceholder(rowIndex * schema.getFields().size() + i + 1).appendString(",")
                );
                sb.removeLast();
                sb.appendString(")").appendString(",");
            });
            sb.removeLast();
        } else if(op.equals(OP.INSERT_OR_DONOTHING)) {
            throw new IllegalArgumentException("H2 does not support INSERT_OR_DONOTHING.");
        }

        return sb.build();
    }

    private static PreparedStatementTemplate createSQLServerStatement(final String table, final Schema schema,
                                         final OP op, final List<String> keyFields,
                                         final int bulkInsertSize) {

        if (op.equals(OP.INSERT) && bulkInsertSize > 1000) {
            throw new IllegalArgumentException("SQLServer supports at most 1000 records per bulk insert.");
        }

        final PreparedStatementTemplate.Builder sb = new PreparedStatementTemplate.Builder();

        if(op.equals(OP.INSERT)) {
            sb.appendString("INSERT INTO ").appendString(table);

            sb.appendString(" (");
            schema.getFields().forEach(f -> sb.appendString(f.name()).appendString(","));
            sb.removeLast();
            sb.appendString(")");

            sb.appendString(" VALUES ");
            IntStream.range(0, bulkInsertSize).forEach(rowIndex -> {
                sb.appendString("(");
                IntStream.range(0, schema.getFields().size()).forEach(
                        i -> sb.appendPlaceholder(rowIndex * schema.getFields().size() + i + 1).appendString(",")
                );
                sb.removeLast();
                sb.appendString(")").appendString(",");
            });
            sb.removeLast();
        } else if(op.equals(OP.INSERT_OR_UPDATE)) {
            throw new IllegalArgumentException("SQLServer does not support INSERT_OR_UPDATE.");
        } else if(op.equals(OP.INSERT_OR_DONOTHING)) {
            throw new IllegalArgumentException("SQLServer does not support INSERT_OR_DONOTHING.");
        }

        return sb.build();
    }

    public static void setStatement(final PreparedStatement statement,
                                    final int parameterIndex,
                                    final Schema fieldSchema,
                                    final Object fieldValue) throws SQLException {

        if(fieldValue == null) {
            return;
        }
        switch (fieldSchema.getType()) {
            case BOOLEAN -> statement.setBoolean(parameterIndex, (Boolean) fieldValue);
            case ENUM, STRING -> statement.setString(parameterIndex, fieldValue.toString());
            case FIXED, BYTES -> {
                if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                    final ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
                    final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(fieldSchema, byteBuffer);
                    statement.setBigDecimal(parameterIndex, decimal);
                } else {
                    statement.setBytes(parameterIndex, ((ByteBuffer) fieldValue).array());
                }
            }
            case INT -> {
                final Integer value = (Integer) fieldValue;
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    statement.setDate(parameterIndex, java.sql.Date.valueOf(LocalDate.ofEpochDay(Long.valueOf(value))));
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    statement.setTime(parameterIndex, Time.valueOf(LocalTime.ofNanoOfDay(Long.valueOf(value) * 1000_000L)));
                } else {
                    statement.setInt(parameterIndex, value);
                }
            }
            case LONG -> {
                final Long value = (Long) fieldValue;
                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(DateTimeUtil.toLocalDateTime(value * 1000L)));
                } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    statement.setTimestamp(parameterIndex, java.sql.Timestamp.valueOf(DateTimeUtil.toLocalDateTime(value)));
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    statement.setTime(parameterIndex, Time.valueOf(LocalTime.ofNanoOfDay(Long.valueOf(value) * 1000L)));
                } else {
                    statement.setLong(parameterIndex, value);
                }
                break;
            }
            case FLOAT -> statement.setFloat(parameterIndex, (Float) fieldValue);
            case DOUBLE -> statement.setDouble(parameterIndex, (Double) fieldValue);
            case NULL -> {}
            case UNION -> setStatement(statement, parameterIndex, AvroSchemaUtil.unnestUnion(fieldSchema), fieldValue);
            default -> throw new IllegalStateException("Not supported prepare parameter type: " + fieldSchema.getType().getName());
        }
    }

    public static List<String> getPrimaryKeyNames(
            final Connection connection,
            final String database,
            final String namespace,
            final String table) throws SQLException {

        final DatabaseMetaData metaData = connection.getMetaData();
        try (final ResultSet resultSet = metaData.getPrimaryKeys(database, namespace, table)) {
            final Map<Integer,String> primaryKeyNames = new HashMap<>();
            while(resultSet.next()) {
                final Integer primaryKeySeq  = resultSet.getInt("KEY_SEQ");
                final String primaryKeyName = resultSet.getString("COLUMN_NAME");
                primaryKeyNames.put(primaryKeySeq, primaryKeyName);
            }
            if(primaryKeyNames.isEmpty()) {
                LOG.warn("No primary key");
                try(final ResultSet resultSetRowKey = metaData.getBestRowIdentifier(database, namespace, table, DatabaseMetaData.bestRowUnknown, true)) {
                    int i = 0;
                    while(resultSetRowKey.next()) {
                        final Integer primaryKeySeq = i++;
                        final String uniqueKeyName = resultSetRowKey.getString("COLUMN_NAME");
                        primaryKeyNames.put(primaryKeySeq, uniqueKeyName);
                    }
                }
                LOG.info("Unique key size: " + primaryKeyNames.size());
            }
            return primaryKeyNames.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
        }
    }

    public static DB extractDbFromDriver(final String driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver must not be null");
        }
        if(driver.toLowerCase().contains("mysql")) {
            return DB.MYSQL;
        } else if(driver.toLowerCase().contains("postgresql")) {
            return DB.POSTGRESQL;
        } else if(driver.toLowerCase().contains("sqlserver")) {
            return DB.SQLSERVER;
        } else {
            throw new IllegalArgumentException("Not supported database: " + driver);
        }
    }

    private static boolean isValidColumnType(final Schema fieldSchema) {
        return switch (fieldSchema.getType()) {
            case MAP, RECORD -> false;
            case ARRAY -> isValidColumnType(fieldSchema.getElementType());
            case UNION -> isValidColumnType(AvroSchemaUtil.unnestUnion(fieldSchema));
            default -> true;
        };
    }

    private static String getColumnType(final Schema schema, final DB db, final boolean isPrimaryKey) {
        final Schema avroSchema = AvroSchemaUtil.unnestUnion(schema);
        return switch (avroSchema.getType()) {
            case BOOLEAN -> switch (db) {
                case MYSQL -> "TINYINT(1)";
                default -> "BOOLEAN";
            };
            case ENUM -> switch (db) {
                case MYSQL -> "VARCHAR(32) CHARACTER SET utf8mb4";
                default -> "VARCHAR(32)";
            };
            case STRING -> switch (db) {
                case MYSQL -> {
                    if (isPrimaryKey) {
                        yield "VARCHAR(64) CHARACTER SET utf8mb4";
                    } else {
                        yield "TEXT CHARACTER SET utf8mb4";
                    }
                }
                default -> {
                    if (isPrimaryKey) {
                        yield "VARCHAR(64)";
                    } else {
                        yield "TEXT";
                    }
                }
            };
            case FIXED, BYTES -> {
                if (AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    yield "DECIMAL(38, 9)";
                }
                yield switch (db) {
                    case MYSQL -> "MEDIUMBLOB";
                    case POSTGRESQL -> "BYTEA";
                    default -> "BLOB";
                };
            }
            case INT -> {
                if (LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    yield "DATE";
                } else if (LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    yield "TIME";
                } else {
                    yield "INTEGER";
                }
            }
            case LONG -> switch (db) {
                case MYSQL -> {
                    if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                    } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
                    } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIME";
                    } else {
                        yield "BIGINT";
                    }
                }
                case POSTGRESQL -> {
                    if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIME";
                    } else {
                        yield "BIGINT";
                    }
                }
                case SQLSERVER -> {
                    if (LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIMESTAMP";
                    } else if (LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                        yield "TIME";
                    } else {
                        yield "BIGINT";
                    }
                }
                default -> "BIGINT";

            };
            case FLOAT -> "REAL";
            case DOUBLE -> switch (db) {
                case MYSQL -> "DOUBLE";
                case POSTGRESQL -> "DOUBLE PRECISION";
                default -> "DOUBLE";
            };
            default -> throw new IllegalArgumentException(String.format("DataType: %s is not supported!", avroSchema.getType().name()));
        };
    }

    private static String replaceReservedKeyword(final String term) {
        if(RESERVED_KEYWORDS.contains(term.trim().toUpperCase())) {
            return String.format("`%s`", term);
        }
        return term;
    }

    public static class CloseableDataSource implements DataSource, AutoCloseable {
        private final DataSource dataSource;

        public CloseableDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return this.dataSource.getParentLogger();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return this.dataSource.isWrapperFor(iface);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return this.dataSource.unwrap(iface);
        }

        @Override
        public void close() throws IOException {
            if(this.dataSource instanceof Closeable) {
                ((Closeable)this.dataSource).close();
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            return this.dataSource.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return this.dataSource.getConnection(username, password);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return this.dataSource.getLogWriter();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return this.dataSource.getLoginTimeout();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            this.dataSource.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            this.dataSource.setLoginTimeout(seconds);
        }
    }
}
