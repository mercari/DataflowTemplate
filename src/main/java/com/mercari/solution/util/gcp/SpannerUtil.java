package com.mercari.solution.util.gcp;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.mercari.solution.util.RowSchemaUtil;
import com.mercari.solution.util.converter.StructToRowConverter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SpannerUtil {

    public static final String SPANNER_HOST_BATCH = "https://batch-spanner.googleapis.com";
    public static final String SPANNER_HOST_EMULATOR = "http://localhost:9010";

    private static final Logger LOG = LoggerFactory.getLogger(SpannerUtil.class);

    private static final String SQL_SPLITTER = "--SPLITTER--";
    private static final String USERAGENT = "Apache_Beam_Java/" + ReleaseInfo.getReleaseInfo().getVersion();

    private static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            "ALL","AND","ANY","ARRAY","AS","ASC","ASSERT_ROWS_MODIFIED","AT",
            "BETWEEN","BY","CASE","CAST","COLLATE","CONTAINS","CREATE","CROSS","CUBE","CURRENT",
            "DEFAULT","DEFINE","DESC","DISTINCT","ELSE","END","ENUM","ESCAPE","EXCEPT","EXCLUDE","EXISTS","EXTRACT",
            "FALSE","FETCH","FOLLOWING","FOR","FROM","FULL","GROUP","GROUPING","GROUPS","HASH","HAVING",
            "IF","IGNORE","IN","INNER","INTERSECT","INTERVAL","INTO","IS","JOIN",
            "LATERAL","LEFT","LIKE","LIMIT","LOOKUP","MERGE","NATURAL","NEW","NO","NOT","NULL","NULLS",
            "OF","ON","OR","ORDER","OUTER","OVER","PARTITION","PRECEDING","PROTO","RANGE");

    private static final Pattern PATTERN_ARRAY_ELEMENT = Pattern.compile("(?<=\\<).*?(?=\\>)");

    private static final Pattern PATTERN_DATE1 = Pattern.compile("[0-9]{8}");
    private static final Pattern PATTERN_DATE2 = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final Pattern PATTERN_DATE3 = Pattern.compile("[0-9]{4}/[0-9]{2}/[0-9]{2}");


    public static Spanner connectSpanner(final String projectId,
                                         final int channel,
                                         final int sessionMin,
                                         final int sessionMax,
                                         final boolean batch,
                                         final boolean emulator) {

        final SpannerOptions.Builder builder = SpannerOptions.newBuilder()
                .setNumChannels(channel)
                .setSessionPoolOption(SessionPoolOptions.newBuilder()
                        .setMinSessions(sessionMin)
                        .setMaxSessions(sessionMax)
                        .build())
                .setHeaderProvider(FixedHeaderProvider.create("user-agent", USERAGENT))
                .setProjectId(projectId);

        final RetrySettings retrySettings =
                RetrySettings.newBuilder()
                        .setInitialRpcTimeout(Duration.ofHours(2))
                        .setMaxRpcTimeout(Duration.ofHours(2))
                        .setMaxAttempts(5)
                        .setTotalTimeout(Duration.ofHours(2))
                        .build();

        try {
            builder.getSpannerStubSettingsBuilder()
                    .applyToAllUnaryMethods(input -> {
                        input.setRetrySettings(retrySettings);
                        return null;
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if(emulator) {
            return builder.setEmulatorHost(SPANNER_HOST_EMULATOR).build().getService();
        }
        if(batch) {
            return builder.setHost(SPANNER_HOST_BATCH).build().getService();
        }
        return builder.build().getService();
    }

    public static boolean hasField(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return false;
        }
        return struct.getType().getStructFields().stream()
                .anyMatch(f -> f.getName().equals(fieldName));
    }

    public static Object getValue(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return struct.getBoolean(field);
            case BYTES:
                return struct.getBytes(field).toBase64();
            case STRING:
                return struct.getString(field);
            case INT64:
                return struct.getLong(field);
            case FLOAT64:
                return struct.getDouble(field);
            case DATE:
                return struct.getDate(field).toString();
            case TIMESTAMP:
                return struct.getTimestamp(field).toString();
            case STRUCT:
                return struct.getStruct(field);
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static String getAsString(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return Boolean.toString(struct.getBoolean(field));
            case BYTES:
                return struct.getBytes(field).toBase64();
            case STRING:
                return struct.getString(field);
            case INT64:
                return Long.toString(struct.getLong(field));
            case FLOAT64:
                return Double.toString(struct.getDouble(field));
            case DATE:
                return struct.getDate(field).toString();
            case TIMESTAMP:
                return struct.getTimestamp(field).toString();
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static byte[] getBytes(final Struct struct, final String fieldName) {
        final Type.StructField field = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny().orElse(null);
        if(field == null) {
            return null;
        }
        switch(field.getType().getCode()) {
            case BYTES:
                return struct.getBytes(field.getName()).toByteArray();
            case STRING:
                return Base64.getDecoder().decode(struct.getString(fieldName));
            default:
                return null;
                //throw new IllegalStateException();
        }
    }

    public static long getEpochDay(final Date date) {
        return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()).toEpochDay();
    }

    public static Instant getTimestamp(final Struct struct, final String field, final Instant timestampDefault) {
        if(struct.isNull(field)) {
            return timestampDefault;
        }
        switch (struct.getColumnType(field).getCode()) {
            case STRING: {
                final String stringValue = struct.getString(field);
                try {
                    return Instant.parse(stringValue);
                } catch (Exception e) {
                    if(PATTERN_DATE1.matcher(stringValue).find()) {
                        return new DateTime(
                                Integer.valueOf(stringValue.substring(0, 4)),
                                Integer.valueOf(stringValue.substring(4, 6)),
                                Integer.valueOf(stringValue.substring(6, 8)),
                                0, 0, DateTimeZone.UTC).toInstant();
                    }

                    Matcher matcher = PATTERN_DATE2.matcher(stringValue);
                    if(matcher.find()) {
                        final String[] values = matcher.group().split("-");
                        return new DateTime(
                                Integer.valueOf(values[0]),
                                Integer.valueOf(values[1]),
                                Integer.valueOf(values[2]),
                                0, 0, DateTimeZone.UTC).toInstant();
                    }
                    matcher = PATTERN_DATE3.matcher(stringValue);
                    if(matcher.find()) {
                        final String[] values = matcher.group().split("/");
                        return new DateTime(
                                Integer.valueOf(values[0]),
                                Integer.valueOf(values[1]),
                                Integer.valueOf(values[2]),
                                0, 0, DateTimeZone.UTC).toInstant();
                    }
                    return timestampDefault;
                }
            }
            case DATE: {
                final Date date = struct.getDate(field);
                return new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(),
                        0, 0, DateTimeZone.UTC).toInstant();
            }
            case TIMESTAMP:
                return Instant.ofEpochMilli(struct.getTimestamp(field).toSqlTimestamp().getTime());
            case INT64:
            case FLOAT64:
            case BOOL:
            case BYTES:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static Timestamp toCloudTimestamp(final Instant instant) {
        if(instant == null) {
            return null;
        }
        return Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
    }

    public static Type addStructField(final Type type, final List<Type.StructField> fields) {
        final List<Type.StructField> allFields = type.getStructFields();
        allFields.addAll(fields);
        return Type.struct(allFields);
    }

    public static Struct.Builder toBuilder(final Struct struct) {
        return toBuilder(struct, null, null);
    }

    public static Struct.Builder toBuilder(final Struct struct,
                                           final Collection<String> includeFields,
                                           final Collection<String> excludeFields) {

        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(includeFields != null && !includeFields.contains(field.getName())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }
            switch (field.getType().getCode()) {
                case BOOL:
                    builder.set(field.getName()).to(struct.getBoolean(field.getName()));
                    break;
                case STRING:
                    builder.set(field.getName()).to(struct.getString(field.getName()));
                    break;
                case BYTES:
                    builder.set(field.getName()).to(struct.getBytes(field.getName()));
                    break;
                case INT64:
                    builder.set(field.getName()).to(struct.getLong(field.getName()));
                    break;
                case FLOAT64:
                    builder.set(field.getName()).to(struct.getDouble(field.getName()));
                    break;
                case NUMERIC:
                    builder.set(field.getName()).to(struct.getBigDecimal(field.getName()));
                    break;
                case TIMESTAMP:
                    builder.set(field.getName()).to(struct.getTimestamp(field.getName()));
                    break;
                case DATE:
                    builder.set(field.getName()).to(struct.getDate(field.getName()));
                    break;
                case STRUCT:
                    builder.set(field.getName()).to(struct.getStruct(field.getName()));
                    break;
                case ARRAY: {
                    switch (field.getType().getArrayElementType().getCode()) {
                        case FLOAT64:
                            builder.set(field.getName()).toFloat64Array(struct.getDoubleList(field.getName()));
                            break;
                        case BOOL:
                            builder.set(field.getName()).toBoolArray(struct.getBooleanList(field.getName()));
                            break;
                        case INT64:
                            builder.set(field.getName()).toInt64Array(struct.getLongList(field.getName()));
                            break;
                        case STRING:
                            builder.set(field.getName()).toStringArray(struct.getStringList(field.getName()));
                            break;
                        case BYTES:
                            builder.set(field.getName()).toBytesArray(struct.getBytesList(field.getName()));
                            break;
                        case DATE:
                            builder.set(field.getName()).toDateArray(struct.getDateList(field.getName()));
                            break;
                        case TIMESTAMP:
                            builder.set(field.getName()).toTimestampArray(struct.getTimestampList(field.getName()));
                            break;
                        case NUMERIC:
                            builder.set(field.getName()).toNumericArray(struct.getBigDecimalList(field.getName()));
                            break;
                        case STRUCT:
                            builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), struct.getStructList(field.getName()));
                            break;
                        case ARRAY:
                            throw new IllegalStateException("Array in Array not supported for spanner struct: " + struct.getType());
                        default:
                            break;
                    }
                    break;
                }
                default:
                    break;
            }
        }
        return builder;
    }

    public static boolean existsTable(final Spanner spanner, final DatabaseId databaseId, final String table) {
        final DatabaseClient client = spanner.getDatabaseClient(databaseId);
        try(final ReadOnlyTransaction transaction = client.singleUseReadOnlyTransaction();
            final ResultSet resultSet = transaction.executeQuery(Statement.newBuilder(
                    "SELECT table_name FROM information_schema.tables WHERE table_name=@table")
                    .bind("table")
                    .to(table)
                    .build())) {

            return resultSet.next();
        }
    }

    public static void createTable(final Spanner spanner,
                                   final String instanceId,
                                   final String databaseId,
                                   final String table,
                                   final Schema schema,
                                   final List<String> primaryKeyFields,
                                   final String interleavedIn,
                                   final boolean cascade,
                                   final boolean wait) {

        final String createTableSQL = buildCreateTableSQL(schema, table, primaryKeyFields, interleavedIn, cascade);
        final OperationFuture<Void, UpdateDatabaseDdlMetadata> meta = spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(instanceId, databaseId, Arrays.asList(createTableSQL), null);

        try {
            meta.get();
            if(wait) {
                int waitingSeconds = 0;
                while (!meta.isDone()) {
                    Thread.sleep(5 * 1000L);
                    waitingSeconds += 5;
                    if (waitingSeconds > 3600) {
                        throw new IllegalArgumentException("Timeout creating table: " + table);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable(final Spanner spanner,
                                   final String instanceId, final String databaseId, final String table,
                                   final Schema schema, final List<String> primaryKeyFields) {

        createTable(spanner, instanceId, databaseId, table, schema, primaryKeyFields, null, true, false);
    }

    public static void deleteTable(final Spanner spanner,
                                   final String instanceId, final String databaseId, final String table) {
        final String sql = String.format("DROP TABLE %s", table);
        final OperationFuture<Void, UpdateDatabaseDdlMetadata> meta = spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(instanceId, databaseId, Arrays.asList(sql), null);

        try {
            meta.get();
            int waitingSeconds = 0;
            while (!meta.isDone()) {
                Thread.sleep(5 * 1000L);
                waitingSeconds += 5;
                if (waitingSeconds > 3600) {
                    throw new IllegalArgumentException("");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long emptyTable(final Spanner spanner,
                                  final String projectId, final String instanceId, final String databaseId, final String table) {

        final String sql = String.format("DELETE FROM %s WHERE TRUE", table);
        return spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId))
                .executePartitionedUpdate(Statement.of(sql));
    }

    public static Schema getSchemaFromQuery(final String projectId,
                                            final String instanceId,
                                            final String databaseId,
                                            final String query,
                                            final boolean emulator) {

        final Type type = getTypeFromQuery(projectId, instanceId, databaseId, query, emulator);
        return StructToRowConverter.convertSchema(type);
    }

    public static Type getTypeFromQuery(final String projectId,
                                        final String instanceId,
                                        final String databaseId,
                                        final String query,
                                        final boolean emulator) {

        try(final Spanner spanner = connectSpanner(projectId, 1, 1, 1, false, emulator)) {
            final DatabaseId database = DatabaseId.of(projectId, instanceId, databaseId);
            final DatabaseClient client = spanner.getDatabaseClient(database);
            try(final ReadOnlyTransaction transaction = client.singleUseReadOnlyTransaction();
                final ResultSet resultSet = transaction.analyzeQuery(Statement.of(query.split(SQL_SPLITTER)[0]), ReadContext.QueryAnalyzeMode.PLAN)) {

                resultSet.next();
                return resultSet.getType();
            }
        }
    }

    public static Schema getSchemaFromTable(final String projectId,
                                            final String instanceId,
                                            final String databaseId,
                                            final String table,
                                            final boolean emulator) {

        return getSchemaFromTable(projectId, instanceId, databaseId, table, null, emulator);
    }

    public static Schema getSchemaFromTable(final String projectId,
                                            final String instanceId,
                                            final String databaseId,
                                            final String table,
                                            final Collection<String> includeColumns,
                                            final boolean emulator) {

        final List<Struct> structs = getSchemaFieldsFromTable(projectId, instanceId, databaseId, table, emulator);
        return convertSchemaFromInformationSchema(structs, includeColumns);
    }

    public static Type getTypeFromTable(final String projectId,
                                        final String instanceId,
                                        final String databaseId,
                                        final String table,
                                        final Collection<String> includeColumns,
                                        final boolean emulator) {

        final List<Struct> structs = getSchemaFieldsFromTable(projectId, instanceId, databaseId, table, emulator);
        return convertTypeFromInformationSchema(structs, includeColumns);
    }

    public static List<String> getPrimaryKeyFieldNames(final String projectId,
                                                       final String instanceId,
                                                       final String databaseId,
                                                       final String table,
                                                       final boolean emulator) {
        final List<Struct> structs = getPrimaryKeyFieldsFromTable(projectId, instanceId, databaseId, table, emulator);
        return structs.stream()
                .map(s -> s.getString("COLUMN_NAME"))
                .collect(Collectors.toList());
    }

    private static List<Struct> getSchemaFieldsFromTable(final String projectId,
                                                         final String instanceId,
                                                         final String databaseId,
                                                         final String table,
                                                         final boolean emulator) {

        final DatabaseId database = DatabaseId.of(projectId, instanceId, databaseId);
        final String query = String.format("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'", table);
        try(final Spanner spanner = connectSpanner(projectId, 1, 1, 1, false, emulator)) {
            final DatabaseClient client = spanner.getDatabaseClient(database);
            try(final ReadOnlyTransaction transaction = client.singleUseReadOnlyTransaction();
                final ResultSet resultSet = transaction.executeQuery(Statement.of(query))) {

                final List<Struct> structs = new ArrayList<>();
                while(resultSet.next()) {
                    Struct struct = resultSet.getCurrentRowAsStruct();
                    structs.add(struct);
                }
                return structs;
            }
        }
    }

    private static List<Struct> getPrimaryKeyFieldsFromTable(final String projectId,
                                                             final String instanceId,
                                                             final String databaseId,
                                                             final String table,
                                                             final boolean emulator) {

        final DatabaseId database = DatabaseId.of(projectId, instanceId, databaseId);
        final String query = String.format("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='%s' ORDER BY ORDINAL_POSITION", table);
        try(final Spanner spanner = connectSpanner(projectId, 1, 1, 1, false, emulator)) {
            final DatabaseClient client = spanner.getDatabaseClient(database);
            try(final ReadOnlyTransaction transaction = client.singleUseReadOnlyTransaction();
                final ResultSet resultSet = transaction.executeQuery(Statement.of(query))) {

                final List<Struct> structs = new ArrayList<>();
                while(resultSet.next()) {
                    Struct struct = resultSet.getCurrentRowAsStruct();
                    if(struct.getString("CONSTRAINT_NAME").startsWith("PK_")) {
                        structs.add(struct);
                    }
                }
                return structs;
            }
        }
    }

    public static Schema convertSchemaFromInformationSchema(final List<Struct> structs, final Collection<String> columnNames) {
        final Schema.Builder builder = Schema.builder();
        for(final Struct struct : structs) {
            if(columnNames != null && !columnNames.contains(struct.getString("COLUMN_NAME"))) {
                LOG.info("skipField: " + struct.getString("COLUMN_NAME"));
                continue;
            } else {
                LOG.info("includeField: " + struct.getString("COLUMN_NAME"));
            }
            builder.addField(Schema.Field.of(
                    struct.getString("COLUMN_NAME"),
                    convertFieldType(struct.getString("SPANNER_TYPE")))
                    .withNullable("YES".equals(struct.getString("IS_NULLABLE"))));
        }
        return builder.build();
    }

    private static Type convertTypeFromInformationSchema(final List<Struct> structs, final Collection<String> columnNames) {
        final List<Type.StructField> fields = new ArrayList<>();
        for(final Struct struct : structs) {
            if(columnNames != null && !columnNames.contains(struct.getString("COLUMN_NAME"))) {
                LOG.info("skipField: " + struct.getString("COLUMN_NAME"));
                continue;
            } else {
                LOG.info("includeField: " + struct.getString("COLUMN_NAME"));
            }
            fields.add(Type.StructField.of(
                    struct.getString("COLUMN_NAME"),
                    convertSchemaField(struct.getString("SPANNER_TYPE"))));
        }
        return Type.struct(fields);
    }

    public static void executeDdl(final Spanner spanner, final String instanceId, final String databaseId, final String ddl) {
        executeDdl(spanner, instanceId, databaseId, ddl, 5);
    }

    private static void executeDdl(final Spanner spanner, final String instanceId, final String databaseId,
                                   final String ddl, final int num) {

        final OperationFuture<Void, UpdateDatabaseDdlMetadata> meta = spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(instanceId, databaseId, Arrays.asList(ddl), null);
        try {
            meta.get(60, TimeUnit.SECONDS);
            int waitingSeconds = 0;
            while (!meta.isDone()) {
                Thread.sleep(5 * 1000L);
                waitingSeconds += 5;
                if (waitingSeconds > 3600) {
                    throw new IllegalArgumentException("Timeout execute ddl: " + ddl);
                }
            }
        } catch (Exception e) {
            if(num < 0) {
                throw new RuntimeException(e);
            }
            LOG.warn("Failed to execute ddl: " + ddl + ", cause: " + e.getMessage() + ", retry: " + num);
            try {
                Thread.sleep(5 * 1000L);
            } catch (InterruptedException ee) {

            }
            executeDdl(spanner, instanceId, databaseId, ddl, num - 1);
        }
    }

    public static String buildCreateTableSQL(final Schema schema,
                                              final String table,
                                              final List<String> primaryKeyFields,
                                              final String interleavedIn,
                                              final boolean cascade) {

        if(primaryKeyFields == null && !schema.getOptions().hasOption("spannerPrimaryKey")) {
            throw new IllegalArgumentException("Runtime parameter: primaryKeyFields must not be null!");
        }
        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE %s ( ", table));
        schema.getFields().stream()
                .filter(f -> isValidColumnType(f.getType()))
                .forEach(f -> sb.append(String.format("%s %s%s,",
                        replaceReservedKeyword(f.getName()),
                        f.getOptions().hasOption("sqlType") ? f.getOptions().getValue("sqlType") : getColumnType(f.getType()),
                        f.getType().getNullable() == null || f.getType().getNullable() ? "" : " NOT NULL")));
        sb.deleteCharAt(sb.length() - 1);
        final String primaryKey;
        if(primaryKeyFields == null) {
            primaryKey = schema.getOptions().getValue("spannerPrimaryKey");
        } else {
            primaryKey = StringUtils.join(replaceReservedKeyword(primaryKeyFields), ",");
        }
        sb.append(String.format(") PRIMARY KEY ( %s )", primaryKey));
        if(interleavedIn != null) {
            sb.append(",");
            sb.append("INTERLEAVE IN PARENT ");
            sb.append(interleavedIn);
            sb.append(String.format(" ON DELETE %s", cascade ? "CASCADE" : "NO ACTION"));
        }
        return sb.toString();
    }

    public static String buildDropTableSQL(final String table) {
        return String.format("DROP TABLE %s", table);
    }

    public static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final String mutationOp) {
        if(mutationOp == null) {
            return Mutation.newInsertOrUpdateBuilder(table);
        }
        switch(mutationOp.trim().toUpperCase()) {
            case "INSERT":
                return Mutation.newInsertBuilder(table);
            case "UPDATE":
                return Mutation.newUpdateBuilder(table);
            case "INSERT_OR_UPDATE":
                return Mutation.newInsertOrUpdateBuilder(table);
            case "REPLACE":
                return Mutation.newReplaceBuilder(table);
            case "DELETE":
                throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }


    private static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final Mutation.Op mutationOp) {
        switch(mutationOp) {
            case INSERT:
                return Mutation.newInsertBuilder(table);
            case UPDATE:
                return Mutation.newUpdateBuilder(table);
            case INSERT_OR_UPDATE:
                return Mutation.newInsertOrUpdateBuilder(table);
            case REPLACE:
                return Mutation.newReplaceBuilder(table);
            case DELETE:
                throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }

    public static <InputT> Mutation createDeleteMutation(
            final InputT element,
            final String table, final Iterable<String> keyFields,
            final ValueGetter<InputT> function) {

        if(keyFields == null) {
            throw new IllegalArgumentException("keyFields is null. Set keyFields when using mutationOp:DELETE");
        }
        Key.Builder keyBuilder = Key.newBuilder();
        for(final String keyField : keyFields) {
            keyBuilder = keyBuilder.appendObject(function.convert(element, keyField));
        }
        return Mutation.delete(table, keyBuilder.build());
    }

    private static boolean isValidColumnType(final Schema.FieldType fieldType) {
        switch (fieldType.getTypeName()) {
            case MAP:
            case ROW:
                return false;
            case ITERABLE:
            case ARRAY:
                if(!isValidColumnType(fieldType.getCollectionElementType())) {
                    return false;
                }
                return true;
            default:
                return true;
        }
    }

    private static String getColumnType(final Schema.FieldType fieldType) {
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return "BOOL";
            case STRING:
                return "STRING(MAX)";
            case DECIMAL:
            case BYTES:
                return "BYTES(MAX)";
            case INT16:
            case INT32:
            case INT64:
                return "INT64";
            case FLOAT:
            case DOUBLE:
                return "FLOAT64";
            case DATETIME:
                return "TIMESTAMP";
            case ITERABLE:
            case ARRAY:
                return "ARRAY<" + getColumnType(fieldType.getCollectionElementType()) + ">";
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return "DATE";
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return "STRING(MAX)";
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return "TIMESTAMP";
                }
                throw new IllegalArgumentException(String.format("FieldType: %s, LogicalType base: %s, argument: %s is not supported!",
                        fieldType.toString(),
                        fieldType.getLogicalType().getBaseType().getTypeName().name(),
                        fieldType.getLogicalType().getArgumentType().getTypeName().name()));
            case ROW:
            case MAP:
            case BYTE:
            default:
                throw new IllegalArgumentException(String.format("DataType: %s is not supported!", fieldType.getTypeName().name()));

        }
    }

    private static Schema.FieldType convertFieldType(final String t) {
        final String type = t.trim().toUpperCase();
        switch (type) {
            case "INT64":
                return Schema.FieldType.INT64;
            case "FLOAT64":
                return Schema.FieldType.DOUBLE;
            case "BOOL":
                return Schema.FieldType.BOOLEAN;
            case "DATE":
                return CalciteUtils.DATE;
            case "TIMESTAMP":
                return Schema.FieldType.DATETIME;
            case "BYTES":
                return Schema.FieldType.BYTES;
            default:
                if(type.startsWith("STRING")) {
                    return Schema.FieldType.STRING;
                } else if(type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if(m.find()) {
                        return Schema.FieldType.array(convertFieldType(m.group()).withNullable(true));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
        }
    }

    private static Type convertSchemaField(final String t) {
        final String type = t.trim().toUpperCase();
        switch (type) {
            case "INT64":
                return Type.int64();
            case "FLOAT64":
                return Type.float64();
            case "BOOL":
                return Type.bool();
            case "DATE":
                return Type.date();
            case "TIMESTAMP":
                return Type.timestamp();
            default:
                if(type.startsWith("STRING")) {
                    return Type.string();
                } else if(type.startsWith("BYTES")) {
                    return Type.bytes();
                } else if(type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if(m.find()) {
                        return Type.array(convertSchemaField(m.group()));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
        }
    }

    private static String replaceReservedKeyword(final String term) {
        if(RESERVED_KEYWORDS.contains(term.trim().toUpperCase())) {
            return String.format("`%s`", term);
        }
        return term;
    }

    private static List<String> replaceReservedKeyword(final List<String> terms) {
        return terms.stream()
                .map(term -> {
                    if(RESERVED_KEYWORDS.contains(term.trim().toUpperCase())){
                        return String.format("`%s`", term);
                    } else {
                        return term;
                    }
                })
                .collect(Collectors.toList());
    }

    public interface ValueGetter<InputT> extends Serializable {
        Object convert(InputT element, String fieldName);
    }

}
