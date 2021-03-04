package com.mercari.solution.util.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.converter.TableRecordToRowConverter;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class BigQueryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtil.class);

    public static TableReference getTableReference(final String tableName, final String defaultProjectId) {
        final String[] path = tableName.replaceAll(":", ".").split("\\.");
        if(path.length < 2) {
            throw new IllegalArgumentException("table: " + tableName + " is illegal. table must be {projectId}.{datasetId}.{tableId}");
        }

        final String projectId = path.length == 2 ? defaultProjectId : path[0];
        final String datasetId = path[path.length - 2];
        final String tableId   = path[path.length - 1];

        return new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
    }

    public static DatasetReference getDatasetReference(final String datasetName, final String defaultProjectId) {
        final String[] path = datasetName.replaceAll(":", ".").split("\\.");
        if(path.length < 1) {
            throw new IllegalArgumentException("dataset: " + datasetName + " is illegal. table must be {projectId}.{datasetId}");
        }

        final String projectId = path.length == 1 ? defaultProjectId : path[0];
        final String datasetId = path[path.length - 1];

        return new DatasetReference().setProjectId(projectId).setDatasetId(datasetId);
    }

    public static Schema getSchemaFromQuery(final String projectId, final String query) {
        return TableRecordToRowConverter.convertSchema(getTableSchemaFromQuery(projectId, query));
    }

    public static org.apache.avro.Schema getAvroSchemaFromQuery(final String projectId, final String query) {
        return AvroSchemaUtil.convertSchema(getTableSchemaFromQuery(projectId, query));
    }

    public static TableSchema getTableSchemaFromQuery(final String projectId, final String query) {
        final Job job = getQueryDryRunJob(projectId, query);
        return job.getStatistics().getQuery().getSchema();
    }

    private static Job getQueryDryRunJob(final String projectId, final String query) {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = new JacksonFactory();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            final Bigquery bigquery = new Bigquery.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("BigQueryClient")
                    .build();

            final String queryRunProjectId;
            if(projectId != null) {
                queryRunProjectId = projectId;
            } else {
                queryRunProjectId = getUserDefaultProject(credential);
            }

            return getQueryDryRunJob(bigquery, queryRunProjectId, query);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Job getQueryDryRunJob(final Bigquery bigquery, final String projectId, final String query) {
        try {
            return bigquery.jobs().insert(projectId, new Job()
                    .setConfiguration(new JobConfiguration()
                            .setQuery(new JobConfigurationQuery()
                                    .setQuery(query)
                                    .setUseLegacySql(false))
                            .setDryRun(true)))
                    .execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Schema getSchemaFromTable(final String tableName, final String defaultProjectId) {
        return getSchemaFromTable(tableName, defaultProjectId, null);
    }

    public static Schema getSchemaFromTable(final String tableName, final String defaultProjectId, final Collection<String> fields) {
        return TableRecordToRowConverter.convertSchema(getTableSchemaFromTable(tableName, defaultProjectId), fields);
    }

    public static TableSchema getTableSchemaFromTable(final String tableName, final String defaultProjectId) {
        final Bigquery bigquery = getBigquery();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final String queryRunProjectId;
            if(defaultProjectId != null) {
                queryRunProjectId = defaultProjectId;
            } else {
                queryRunProjectId = getUserDefaultProject(credential);
            }

            final TableReference tableReference = getTableReference(tableName, queryRunProjectId);

            final Table table = bigquery.tables()
                    .get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId())
                    .execute();
            return table.getSchema();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static org.apache.avro.Schema getTableSchemaFromTableStorage(final TableReference table, final String project) {
        return getTableSchemaFromTableStorage(table, project, null, null);
    }

    public static org.apache.avro.Schema getTableSchemaFromTableStorage(
            final String tableName, final String project, final List<String> fields, final String restriction) {

        final TableReference table = getTableReference(tableName, project);
        return getTableSchemaFromTableStorage(table, project, fields, restriction);
    }

    public static org.apache.avro.Schema getTableSchemaFromTableStorage(
            final TableReference table, final String project, final List<String> fields, final String restriction) {

        try(final BigQueryReadClient client = BigQueryReadClient.create()) {

            final String srcTable = String.format(
                    "projects/%s/datasets/%s/tables/%s",
                    table.getProjectId(), table.getDatasetId(), table.getTableId());
            final String parent = String.format("projects/%s", project);

            ReadSession.TableReadOptions.Builder options = ReadSession.TableReadOptions.newBuilder();
            if(fields != null) {
                options = options.addAllSelectedFields(fields);
            }
            if(restriction != null) {
                options = options.setRowRestriction(restriction);
            }

            final ReadSession.Builder sessionBuilder =
                    ReadSession.newBuilder()
                            .setTable(srcTable)
                            .setDataFormat(DataFormat.AVRO)
                            .setReadOptions(options);

            final CreateReadSessionRequest.Builder builder =
                    CreateReadSessionRequest.newBuilder()
                            .setParent(parent)
                            .setReadSession(sessionBuilder)
                            .setMaxStreamCount(1);

            final ReadSession session = client.createReadSession(builder.build());
            return new org.apache.avro.Schema.Parser().parse(session.getAvroSchema().getSchema());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Bigquery getBigquery() {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = new JacksonFactory();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            return new Bigquery.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("BigQueryClient")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    private static String getUserDefaultProject(final Credentials credential) {
        if(credential instanceof UserCredentials) {
            return ((UserCredentials)credential).getQuotaProjectId();
        }
        return null;
    }

    public static Job pollJob(final Bigquery bigquery,
                               final JobReference jobRef,
                               final Sleeper sleeper,
                               final BackOff backoff) {
        do {
            try {
                final Job job = bigquery.jobs()
                        .get(jobRef.getProjectId(), jobRef.getJobId())
                        .setLocation(jobRef.getLocation())
                        .execute();
                if (job == null) {
                    LOG.info("Still waiting for BigQuery job {} to start", jobRef);
                    continue;
                }
                JobStatus status = job.getStatus();
                if (status == null) {
                    LOG.info("Still waiting for BigQuery job {} to enter pending state", jobRef);
                    continue;
                }
                if ("DONE".equals(status.getState())) {
                    LOG.info("BigQuery job {} completed in state DONE", jobRef);
                    return job;
                }
                LOG.info("Still waiting for BigQuery job {}, currently in status {}", jobRef.getJobId(), status);
            } catch (IOException e) {
                LOG.info("Ignore the error and retry polling job status.", e);
            }
        } while (nextBackOff(sleeper, backoff));
        LOG.warn("Unable to poll job status: {}, aborting after reached max .", jobRef.getJobId());
        return null;
    }

    private static boolean nextBackOff(Sleeper sleeper, BackOff backoff) {
        try {
            return BackOffUtils.next(sleeper, backoff);
        } catch (InterruptedException | IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }

    public static boolean isJobResultSucceeded(final Job job) {
        if(job == null) {
            return false;
        }
        if(job.getStatus().getErrorResult() != null) {
            return false;
        }
        if(job.getStatus().getErrors() != null && !job.getStatus().getErrors().isEmpty()) {
            return false;
        }
        return true;
    }

}
