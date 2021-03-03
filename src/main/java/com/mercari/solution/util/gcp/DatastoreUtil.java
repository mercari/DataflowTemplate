package com.mercari.solution.util.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableSet;
import com.google.datastore.v1.*;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.rpc.Code;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    public static Schema getSchema(final PipelineOptions pipelineOptions, final String project, final String kind) {
        final GcpOptions gcpOptions = pipelineOptions.as(GcpOptions.class);
        final Datastore datastore = getDatastore(project, gcpOptions.getGcpCredential());

        final String gql = String.format("SELECT * FROM __Stat_PropertyType_PropertyName_Kind__ WHERE kind_name = '%s'", kind);
        try {
            final List<Entity> entities = query(datastore, project, gql);
            return EntitySchemaUtil.convertSchema("", entities);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
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
