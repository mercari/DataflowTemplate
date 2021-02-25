package com.mercari.solution.util.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.Date;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableSet;
import com.google.datastore.v1.*;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DatastoreUtil {

    public static final int QUOTE_VALUE_SIZE = 1500;

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreUtil.class);

    private static final String HOST = "batch-datastore.googleapis.com";
    private static final int MAX_RETRIES = 5;

    private static final FluentBackoff RUNQUERY_BACKOFF =
            FluentBackoff.DEFAULT
                    .withMaxRetries(MAX_RETRIES)
                    .withInitialBackoff(Duration.standardSeconds(5));

    private static final Set<Code> NON_RETRYABLE_ERRORS =
            ImmutableSet.of(
                    Code.FAILED_PRECONDITION,
                    Code.INVALID_ARGUMENT,
                    Code.PERMISSION_DENIED,
                    Code.UNAUTHENTICATED);

    private static final Pattern PATTERN_DATE1 = Pattern.compile("[0-9]{8}");
    private static final Pattern PATTERN_DATE2 = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final Pattern PATTERN_DATE3 = Pattern.compile("[0-9]{4}/[0-9]{2}/[0-9]{2}");

    public static Object getFieldValue(Entity entity, String fieldName) {
        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(fieldName))
                .map(Map.Entry::getValue)
                .map(DatastoreUtil::getValue)
                .findAny()
                .orElse(null);
    }

    public static String getFieldValueAsString(Entity entity, String fieldName) {
        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(fieldName))
                .map(Map.Entry::getValue)
                .map(DatastoreUtil::getValue)
                .map(v -> v == null ? null : v.toString())
                .findAny()
                .orElse(null);
    }

    public static Object getKeyFieldValue(final Entity entity, String fieldName) {
        final Key.PathElement pe = entity.getKey().getPath(entity.getKey().getPathCount()-1);
        return pe.getName() == null ? pe.getId() : pe.getName();
    }

    public static Object getValue(Value value) {
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case KEY_VALUE: return value.getKeyValue();
            case STRING_VALUE: return value.getStringValue();
            case BLOB_VALUE: return value.getBlobValue();
            case INTEGER_VALUE: return value.getIntegerValue();
            case DOUBLE_VALUE: return value.getDoubleValue();
            case BOOLEAN_VALUE: return value.getBooleanValue();
            case TIMESTAMP_VALUE: return value.getTimestampValue();
            case ENTITY_VALUE: return value.getEntityValue();
            case ARRAY_VALUE: return value.getArrayValue();
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
                return null;
            case GEO_POINT_VALUE:
            default:
                throw new IllegalArgumentException(String.format("%s is not supported!", value.getValueTypeCase().name()));
        }
    }

    public static String getAsString(final Value value) {
        final Object object = getValue(value);
        if(object == null) {
            return null;
        }
        return object.toString();
    }

    public static Date convertDate(final Value value) {
        if(Value.ValueTypeCase.STRING_VALUE.equals(value.getValueTypeCase())) {
            final String datestr = value.getStringValue();
            if(PATTERN_DATE1.matcher(datestr).find()) {
                return Date.fromYearMonthDay(
                        Integer.valueOf(datestr.substring(0, 4)),
                        Integer.valueOf(datestr.substring(4, 6)),
                        Integer.valueOf(datestr.substring(6, 8))
                );
            } else if(PATTERN_DATE2.matcher(datestr).find() || PATTERN_DATE3.matcher(datestr).find()) {
                return Date.fromYearMonthDay(
                        Integer.valueOf(datestr.substring(0, 4)),
                        Integer.valueOf(datestr.substring(5, 7)),
                        Integer.valueOf(datestr.substring(8, 10))
                );
            } else {
                throw new IllegalArgumentException("Illegal date string: " + datestr);
            }
        } else if(Value.ValueTypeCase.INTEGER_VALUE.equals(value.getValueTypeCase())) {
            final LocalDate localDate = LocalDate.ofEpochDay(value.getIntegerValue());
            return Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
        } else {
            throw new IllegalArgumentException();
        }

    }

    public static Schema getSchema(final PipelineOptions pipelineOptions, final String project, final String kind) {
        final GcpOptions gcpOptions = pipelineOptions.as(GcpOptions.class);
        final Datastore datastore = getDatastore(project, gcpOptions.getGcpCredential());

        final String gql = String.format("SELECT * FROM __Stat_PropertyType_PropertyName_Kind__ WHERE kind_name = '%s'", kind);
        try {
            final List<Entity> entities = query(datastore, project, gql);
            return convertSchema("", entities);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Entity.Builder toBuilder(final Entity entity) {
        return Entity.newBuilder(entity);
    }

    public static Entity.Builder toBuilder(final Entity entity,
                                           final Collection<String> includeFields,
                                           final Collection<String> excludeFields) {

        final Entity.Builder builder = Entity.newBuilder();
        for(var entry : entity.getPropertiesMap().entrySet()) {
            if(includeFields != null && !includeFields.contains(entry.getKey())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(entry.getKey())) {
                continue;
            }
            builder.putProperties(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    public static byte[] getBytes(final Entity entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        final Value value = entity.getPropertiesMap().getOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch (value.getValueTypeCase()) {
            case STRING_VALUE:
                return Base64.getDecoder().decode(value.getStringValue());
            case BLOB_VALUE:
                return value.getBlobValue().toByteArray();
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static Instant getTimestamp(final Entity entity, final String fieldName) {
        return getTimestamp(entity, fieldName, Instant.ofEpochSecond(0L));
    }

    public static Timestamp toProtoTimestamp(final Instant instant) {
        if(instant == null) {
            return null;
        }
        final java.time.Instant jinstant = java.time.Instant.ofEpochMilli(instant.getMillis());
        return Timestamp.newBuilder().setSeconds(jinstant.getEpochSecond()).setNanos(jinstant.getNano()).build();
    }

    public static Instant getTimestamp(final Entity entity, final String fieldName, final Instant timestampDefault) {
        final Value value = entity.getPropertiesMap().get(fieldName);
        if(value == null) {
            return timestampDefault;
        }
        switch (value.getValueTypeCase()) {
            case STRING_VALUE: {
                final String stringValue = value.toString();
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
            case INTEGER_VALUE: {
                try {
                    return Instant.ofEpochMilli(value.getIntegerValue());
                } catch (Exception e){
                    return Instant.ofEpochMilli(value.getIntegerValue() / 1000);
                }
            }
            case TIMESTAMP_VALUE: {
                return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue()));
            }
            case KEY_VALUE:
            case BOOLEAN_VALUE:
            case DOUBLE_VALUE:
            case BLOB_VALUE:
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case NULL_VALUE:
            case VALUETYPE_NOT_SET:
            default:
                return null;
        }
    }

    private static Schema convertSchema(final String prefix, final List<Entity> entities) {
        final Schema.Builder builder = Schema.builder();
        final Map<String, List<Entity>> embeddedFields = new HashMap<>();
        for (final Entity entity : entities) {
            String fieldName = entity
                    .getPropertiesOrThrow("property_name")
                    .getStringValue();
            if(prefix.length() > 0 && fieldName.startsWith(prefix)) {
                fieldName = fieldName.replaceFirst(prefix + "\\.", "");
            }

            if(fieldName.contains(".")) {
                final String embeddedFieldName = fieldName.split("\\.", 2)[0];
                if(!embeddedFields.containsKey(embeddedFieldName)) {
                    embeddedFields.put(embeddedFieldName, new ArrayList<>());
                }
                embeddedFields.get(embeddedFieldName).add(entity);
                continue;
            }

            final String fieldType = entity.getPropertiesOrThrow("property_type").getStringValue();
            if("EmbeddedEntity".equals(fieldType) || "NULL".equals(fieldType)) {
                continue;
            }

            builder.addField(Schema.Field.of(
                    fieldName,
                    convertFieldType(fieldType))
                    .withNullable(true));
        }

        if(embeddedFields.size() > 0) {
            for(final Map.Entry<String, List<Entity>> child : embeddedFields.entrySet()) {
                final String childPrefix = (prefix.length() == 0 ? "" : prefix + ".") + child.getKey();
                final Schema childSchema = convertSchema(childPrefix, child.getValue());
                builder.addField(Schema.Field.of(
                        child.getKey(),
                        Schema.FieldType.row(childSchema).withNullable(true)));
            }
        }

        return builder.build();
    }

    private static Schema.FieldType convertFieldType(final String type) {
        switch (type) {
            case "Blob":
            case "ShortBlobg":
                return Schema.FieldType.BYTES.withNullable(true);
            case "IM":
            case "Link":
            case "Email":
            case "User":
            case "PhoneNumber":
            case "PostalAddress":
            case "Category":
            case "Text":
            case "String":
                return Schema.FieldType.STRING;
            case "Rating":
            case "Integer":
                return Schema.FieldType.INT64;
            case "Float":
                return Schema.FieldType.DOUBLE;
            case "Boolean":
                return Schema.FieldType.BOOLEAN;
            case "Date/Time":
                return Schema.FieldType.DATETIME;
            case "EmbeddedEntity":
                //return Schema.FieldType.row(convertSchema(type));
            case "Key":
                //return Schema.FieldType.array(convertFieldType(type.getArrayElementType()));
            case "NULL":
                return Schema.FieldType.STRING;
            default:
                throw new IllegalArgumentException("Spanner type: " + type + " not supported!");
        }
    }



    public static Datastore getDatastore(final PipelineOptions pipelineOptions) {
        final GcpOptions gcpOptions = pipelineOptions.as(GcpOptions.class);
        return getDatastore(gcpOptions.getProject(), gcpOptions.getGcpCredential());
    }

    public static Datastore getDatastore(final String projectId, final Credentials credential) {
        final HttpRequestInitializer initializer;
        if (credential != null) {
            initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential), new RetryHttpRequestInitializer());
        } else {
            initializer = new RetryHttpRequestInitializer();
        }
        return DatastoreFactory.get().create(new DatastoreOptions.Builder()
                .projectId(projectId)
                .initializer(initializer)
                .host(HOST)
                .build());
    }

    public static List<Entity> lookup(final Datastore datastore, final String kind, final Collection<String> ids) throws DatastoreException {
        final List<Key> keys = ids.stream()
                .map(id -> Key.newBuilder()
                        .addPath(Key.PathElement.newBuilder().setKind(kind).setName(id).build())
                        .build())
                .collect(Collectors.toList());
        return lookup(datastore, keys);
    }

    public static Entity lookup(final Datastore datastore, final Key key) throws DatastoreException {
        final LookupResponse resp = datastore.lookup(LookupRequest.newBuilder().addKeys(key).build());
        if(resp.getFoundCount() == 0) {
            return null;
        }
        return resp.getFound(0).getEntity();
    }

    public static List<Entity> lookup(final Datastore datastore, final Collection<Key> keys) throws DatastoreException {
        final LookupResponse resp = datastore.lookup(LookupRequest.newBuilder().addAllKeys(keys).build());
        LOG.info("DatastoreLookup: " + keys.size() +
                ", fetched: " + resp.getFoundList().size() +
                ", missing: " + resp.getMissingList().size());
        final List<Entity> entities = resp.getFoundList().stream()
                .map(EntityResult::getEntity)
                .collect(Collectors.toList());
        if(resp.getDeferredCount() > 0) {
            entities.addAll(lookup(datastore, resp.getDeferredList()));
        }
        return entities;
    }

    public static List<Entity> query(final Datastore datastore,
                                     final String projectId,
                                     final String gql) throws Exception {

        final RunQueryResponse response = datastore
                .runQuery(RunQueryRequest
                        .newBuilder()
                        .setProjectId(projectId)
                        .setGqlQuery(GqlQuery.newBuilder()
                                .setQueryString(gql)
                                .setAllowLiterals(true)
                                .build())
                        .build());

        final Query query = response.getQuery();

        QueryResultBatch currentBatch = response.getBatch();
        final List<Entity> entities = currentBatch.getEntityResultsList().stream()
                .map(EntityResult::getEntity)
                .collect(Collectors.toList());
        boolean moreResults = currentBatch.getMoreResults()
                .equals(QueryResultBatch.MoreResultsType.NOT_FINISHED);

        while(moreResults) {
            final Query.Builder queryBuilder = query.toBuilder();
            if (!currentBatch.getEndCursor().isEmpty()) {
                queryBuilder.setStartCursor(currentBatch.getEndCursor());
            }
            final RunQueryRequest request = RunQueryRequest
                    .newBuilder()
                    .setProjectId(projectId)
                    .setQuery(queryBuilder.build())
                    .build();
            currentBatch = runQueryWithRetries(datastore, request).getBatch();
            currentBatch.getEntityResultsList().stream()
                    .map(EntityResult::getEntity)
                    .forEach(entities::add);
            moreResults = currentBatch.getMoreResults()
                    .equals(QueryResultBatch.MoreResultsType.NOT_FINISHED);
        }
        return entities;
    }

    private static RunQueryResponse runQueryWithRetries(final Datastore datastore,
                                                        final RunQueryRequest request) throws Exception {
        while (true) {
            try {
                RunQueryResponse response = datastore.runQuery(request);
                return response;
            } catch (DatastoreException exception) {
                if (NON_RETRYABLE_ERRORS.contains(exception.getCode())) {
                    throw exception;
                }
                if (!BackOffUtils.next(Sleeper.DEFAULT, RUNQUERY_BACKOFF.backoff())) {
                    LOG.error("Aborting after {} retries.", MAX_RETRIES);
                    throw exception;
                }
            }
        }
    }

}
