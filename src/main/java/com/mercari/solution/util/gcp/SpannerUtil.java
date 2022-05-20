package com.mercari.solution.util.gcp;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.converter.StructToRowConverter;
import org.apache.avro.LogicalTypes;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

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
                        getColumnType(f.getType(), f.getOptions()),
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

    public static String buildCreateTableSQL(final org.apache.avro.Schema schema,
                                             final String table,
                                             final List<String> primaryKeyFields,
                                             final String interleavedIn,
                                             final boolean cascade) {

        if(primaryKeyFields == null || primaryKeyFields.size() == 0) {
            throw new IllegalArgumentException("Runtime parameter: primaryKeyFields must not be null!");
        }
        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE %s ( ", table));
        schema.getFields().stream()
                .filter(f -> isValidColumnType(f.schema()))
                .forEach(f -> sb.append(String.format("%s %s%s,",
                        replaceReservedKeyword(f.name()),
                        getColumnType(f.schema()),
                        AvroSchemaUtil.isNullable(f.schema()) ? "" : " NOT NULL")));
        sb.deleteCharAt(sb.length() - 1);
        final String primaryKey = StringUtils.join(replaceReservedKeyword(primaryKeyFields), ",");
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

    private static boolean isValidColumnType(final org.apache.avro.Schema fieldSchema) {
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

    private static String getColumnType(final Schema.FieldType fieldType, final Schema.Options fieldOptions) {
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return "BOOL";
            case STRING: {
                if(fieldOptions.hasOption("sqlType")) {
                    final String sqlType = fieldOptions.getValue("sqlType");
                    switch (sqlType.toUpperCase()) {
                        case "JSON":
                            return "JSON";
                    }
                }
                return "STRING(MAX)";
            }
            case DECIMAL:
                return "NUMERIC";
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
                return "ARRAY<" + getColumnType(fieldType.getCollectionElementType(), fieldOptions) + ">";
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

    private static String getColumnType(final org.apache.avro.Schema fieldSchema) {
        switch (fieldSchema.getType()) {
            case BOOLEAN:
                return "BOOL";
            case ENUM:
            case STRING:
                return "STRING(MAX)";
            case FIXED:
            case BYTES:
                return "BYTES(MAX)";
            case INT: {
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    return "DATE";
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    return "STRING(MAX)";
                } else {
                    return "INT64";
                }
            }
            case LONG: {
                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    return "TIMESTAMP";
                } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    return "TIMESTAMP";
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    return "STRING(MAX)";
                } else {
                    return "INT64";
                }
            }
            case FLOAT:
            case DOUBLE:
                return "FLOAT64";
            case ARRAY:
                return "ARRAY<" + getColumnType(fieldSchema.getElementType()) + ">";
            case UNION: {
                return getColumnType(AvroSchemaUtil.unnestUnion(fieldSchema));
            }
            case RECORD:
            case MAP:
            default:
                throw new IllegalArgumentException(String.format("DataType: %s is not supported!", fieldSchema));

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
            case "JSON":
                return Type.json();
            case "DATE":
                return Type.date();
            case "TIMESTAMP":
                return Type.timestamp();
            default: {
                if (type.startsWith("STRING")) {
                    return Type.string();
                } else if (type.startsWith("BYTES")) {
                    return Type.bytes();
                } else if (type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if (m.find()) {
                        return Type.array(convertSchemaField(m.group()));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
            }
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

}
