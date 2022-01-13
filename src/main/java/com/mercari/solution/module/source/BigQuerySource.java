package com.mercari.solution.module.source;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.TableRowToRecordConverter;
import com.mercari.solution.util.gcp.BigQueryUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class BigQuerySource implements SourceModule {

    private class BigQuerySourceParameters {

        private String query;
        private String queryPriority;
        private String queryTempDataset;
        private String queryLocation;
        private String table;
        private List<String> fields;
        private String rowRestriction;
        private String kmsKey;

        // for microbatch
        private Integer intervalSecond;
        private Integer gapSecond;
        private Integer maxDurationMinute;
        private Integer catchupIntervalSecond;
        private String startDatetime;
        private String outputCheckpoint;
        private Boolean useCheckpointAsStartDatetime;


        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getQueryPriority() {
            return queryPriority;
        }

        public void setQueryPriority(String queryPriority) {
            this.queryPriority = queryPriority;
        }

        public String getQueryTempDataset() {
            return queryTempDataset;
        }

        public void setQueryTempDataset(String queryTempDataset) {
            this.queryTempDataset = queryTempDataset;
        }

        public String getQueryLocation() {
            return queryLocation;
        }

        public void setQueryLocation(String queryLocation) {
            this.queryLocation = queryLocation;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public String getRowRestriction() {
            return rowRestriction;
        }

        public void setRowRestriction(String rowRestriction) {
            this.rowRestriction = rowRestriction;
        }

        public String getKmsKey() {
            return kmsKey;
        }

        public void setKmsKey(String kmsKey) {
            this.kmsKey = kmsKey;
        }

        public Integer getIntervalSecond() {
            return intervalSecond;
        }

        public void setIntervalSecond(Integer intervalSecond) {
            this.intervalSecond = intervalSecond;
        }

        public Integer getGapSecond() {
            return gapSecond;
        }

        public void setGapSecond(Integer gapSecond) {
            this.gapSecond = gapSecond;
        }

        public Integer getMaxDurationMinute() {
            return maxDurationMinute;
        }

        public void setMaxDurationMinute(Integer maxDurationMinute) {
            this.maxDurationMinute = maxDurationMinute;
        }

        public Integer getCatchupIntervalSecond() {
            return catchupIntervalSecond;
        }

        public void setCatchupIntervalSecond(Integer catchupIntervalSecond) {
            this.catchupIntervalSecond = catchupIntervalSecond;
        }

        public String getStartDatetime() {
            return startDatetime;
        }

        public void setStartDatetime(String startDatetime) {
            this.startDatetime = startDatetime;
        }

        public String getOutputCheckpoint() {
            return outputCheckpoint;
        }

        public void setOutputCheckpoint(String outputCheckpoint) {
            this.outputCheckpoint = outputCheckpoint;
        }

        public Boolean getUseCheckpointAsStartDatetime() {
            return useCheckpointAsStartDatetime;
        }

        public void setUseCheckpointAsStartDatetime(Boolean useCheckpointAsStartDatetime) {
            this.useCheckpointAsStartDatetime = useCheckpointAsStartDatetime;
        }
    }

    public String getName() { return "bigquery"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if (config.getMicrobatch() != null && config.getMicrobatch()) {
            return Collections.singletonMap(config.getName(), BigQuerySource.microbatch(beats, config));
        } else {
            return Collections.singletonMap(config.getName(), BigQuerySource.batch(begin, config, waits));
        }
    }

    public static FCollection<GenericRecord> batch(final PBegin begin, final SourceConfig config, final List<FCollection<?>> wait) {
        if(wait == null) {
            final BigQueryBatchSource source = new BigQueryBatchSource(config);
            final PCollection<GenericRecord> output = begin.apply(config.getName(), source);
            return FCollection.of(config.getName(), output, DataType.AVRO, source.avroSchema);
        } else {
            final BigQueryBatchQueryWaitSource source = new BigQueryBatchQueryWaitSource(config, wait);
            final PCollection<GenericRecord> output = begin.apply(config.getName(), source);
            return FCollection.of(config.getName(), output, DataType.AVRO, source.avroSchema);
        }
    }

    public static FCollection<GenericRecord> microbatch(final PCollection<Long> beats, final SourceConfig config) {
        final BigQueryMicrobatchRead source = new BigQueryMicrobatchRead(config);
        final PCollection<GenericRecord> output = beats.apply(config.getName(), source);
        return FCollection.of(config.getName(), output, DataType.AVRO, source.schema);
    }

    public static class BigQueryBatchSource extends PTransform<PBegin, PCollection<GenericRecord>> {

        private Schema avroSchema;

        private final String timestampAttribute;
        private final String timestampDefault;
        private final BigQuerySourceParameters parameters;
        private final Map<String, Object> templateArgs;

        public BigQuerySourceParameters getParameters() {
            return parameters;
        }

        private BigQueryBatchSource(final SourceConfig config) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
            this.parameters = new Gson().fromJson(config.getParameters(), BigQuerySourceParameters.class);
            this.templateArgs = config.getArgs();
        }

        @Override
        public PCollection<GenericRecord> expand(final PBegin begin) {

            validateParameters(parameters);
            setDefaultParameters(parameters);

            final String project = begin.getPipeline().getOptions().as(GcpOptions.class).getProject();
            final PCollection<GenericRecord> records;
            if(parameters.getQuery() != null) {
                final String query;
                if(parameters.getQuery().startsWith("gs://")) {
                    final String rawQuery = StorageUtil.readString(parameters.getQuery());
                    query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
                } else {
                    query = parameters.getQuery();
                }

                final TableSchema tableSchema = BigQueryUtil.getTableSchemaFromQuery(project, query);
                this.avroSchema = AvroSchemaUtil.convertSchema(tableSchema);

                final BigQueryIO.TypedRead.Method method;
                if(OptionUtil.isDirectRunner(begin.getPipeline().getOptions())) {
                    method = BigQueryIO.TypedRead.Method.EXPORT;
                } else {
                    method = BigQueryIO.TypedRead.Method.DIRECT_READ;
                }

                BigQueryIO.TypedRead<GenericRecord> read = BigQueryIO
                        .read(SchemaAndRecord::getRecord)
                        .fromQuery(query)
                        .usingStandardSql()
                        .useAvroLogicalTypes()
                        .withMethod(method)
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.valueOf(parameters.getQueryPriority()))
                        .withoutValidation()
                        .withCoder(AvroCoder.of(avroSchema));

                if(parameters.getKmsKey() != null) {
                    read = read.withKmsKey(parameters.getKmsKey());
                }
                if(parameters.getQueryTempDataset() != null) {
                    read = read.withQueryTempDataset(parameters.getQueryTempDataset());
                }
                if(parameters.getQueryLocation() != null) {
                    read = read.withQueryLocation(parameters.getQueryLocation());
                }

                records = begin
                        .apply("QueryToBigQuery", read)
                        .setCoder(AvroCoder.of(avroSchema));

            } else if(parameters.getTable() != null) {
                final String[] table = parameters.getTable().trim().replaceAll(":", ".").split("\\.");
                final TableReference tableReference;
                if(table.length == 3) {
                    tableReference = new TableReference().setProjectId(table[0]).setDatasetId(table[1]).setTableId(table[2]);
                } else if(table.length == 2) {
                    tableReference = new TableReference().setProjectId(project).setDatasetId(table[0]).setTableId(table[1]);
                } else {
                    throw new IllegalArgumentException("Illegal table reference format: " + parameters.getTable());
                }

                final List<String> fields;
                if(parameters.getFields() != null) {
                    fields = parameters.getFields().stream()
                            .map(String::trim)
                            .collect(Collectors.toList());
                } else {
                    fields = null;
                }
                this.avroSchema = BigQueryUtil.getTableSchemaFromTableStorage(
                        tableReference, project, fields, parameters.getRowRestriction());

                final BigQueryIO.TypedRead.Method method;
                if(OptionUtil.isDirectRunner(begin.getPipeline().getOptions())) {
                    if(parameters.getFields() != null || parameters.getRowRestriction() != null) {
                        method = BigQueryIO.TypedRead.Method.DIRECT_READ;
                    } else {
                        method = BigQueryIO.TypedRead.Method.EXPORT;
                    }
                } else {
                    method = BigQueryIO.TypedRead.Method.DIRECT_READ;
                }

                BigQueryIO.TypedRead<GenericRecord> read = BigQueryIO
                        .read(SchemaAndRecord::getRecord)
                        .from(tableReference)
                        .useAvroLogicalTypes()
                        .withMethod(method)
                        .withoutValidation()
                        .withCoder(AvroCoder.of(avroSchema));

                if(parameters.getFields() != null) {
                    read = read.withSelectedFields(fields);
                }
                if(parameters.getRowRestriction() != null) {
                    read = read.withRowRestriction(parameters.getRowRestriction());
                }

                records = begin
                        .apply("ReadBigQueryTable", read)
                        .setCoder(AvroCoder.of(avroSchema))
                        .setTypeDescriptor(TypeDescriptor.of(GenericRecord.class));

            } else {
                throw new IllegalArgumentException("bigquery module support only query or table");
            }

            if(timestampAttribute == null) {
                return records;
            } else {
                return records.apply("WithTimestamp", DataTypeTransform
                        .withTimestamp(DataType.AVRO, timestampAttribute, timestampDefault));
            }

        }

        private void validateParameters(final BigQuerySourceParameters parameters) {
            if(parameters == null) {
                throw new IllegalArgumentException("BigQuery SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getQuery() == null && parameters.getTable() == null) {
                errorMessages.add("Parameter must contain query or table");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters(final BigQuerySourceParameters parameters) {
            if(parameters.getQueryPriority() == null) {
                parameters.setQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE.name());
            }
        }

    }

    private static class BigQueryBatchQueryWaitSource extends PTransform<PBegin, PCollection<GenericRecord>> {

        private Schema avroSchema;

        private final String timestampAttribute;
        private final String timestampDefault;
        private final BigQuerySourceParameters parameters;
        private final List<FCollection<?>> wait;

        public BigQuerySourceParameters getParameters() {
            return parameters;
        }

        private BigQueryBatchQueryWaitSource(final SourceConfig config, final List<FCollection<?>> wait) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
            this.wait = wait;
            this.parameters = new Gson().fromJson(config.getParameters(), BigQuerySourceParameters.class);
        }


        @Override
        public PCollection<GenericRecord> expand(final PBegin begin) {
            final String project = begin.getPipeline().getOptions().as(GcpOptions.class).getProject();
            final String query = parameters.getQuery();

            final Bigquery bigquery = BigQueryUtil.getBigquery();
            final Job dryRunJob = BigQueryUtil.getQueryDryRunJob(bigquery, project, query);
            final TableSchema tableSchema = dryRunJob.getStatistics().getQuery().getSchema();
            this.avroSchema = AvroSchemaUtil.convertSchema(tableSchema);

            final String queryLocation;
            if(parameters.getQueryLocation() == null) {
                final TableReference tableReference = dryRunJob.getStatistics().getQuery().getReferencedTables().get(0);
                try {
                    final Table table = bigquery.tables()
                            .get(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId())
                            .execute();
                    queryLocation = table.getLocation();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                queryLocation = parameters.getQueryLocation();
            }

            final List<PCollection<?>> pwait = wait.stream()
                    .map(FCollection::getCollection)
                    .collect(Collectors.toList());

            final String table = UUID.randomUUID().toString().replaceAll("-", "");
            final String dataset;
            final boolean deleteDataset;
            if(parameters.getQueryTempDataset() == null) {
                dataset = table;
                deleteDataset = true;
            } else {
                dataset = parameters.getQueryTempDataset();
                deleteDataset = false;
            }

            final PCollection<GenericRecord> records = begin
                    .apply("Seed", Create.of("seed"))
                    .apply("Wait", Wait.on(pwait))
                    .apply("ExecuteQuery", ParDo.of(new QueryDoFn(
                            project, query, queryLocation, dataset, table, deleteDataset, parameters.getKmsKey())))
                    .setCoder(ProtoCoder.of(ReadStream.class))
                    .apply("ReadRecord", ParDo.of(new ReadDoFn(this.avroSchema.toString())))
                    .setCoder(AvroCoder.of(avroSchema));

            records.apply("Sample", Sample.any(1))
                    .apply("CleanUp", ParDo.of(new CleanDoFn(project, dataset, table, deleteDataset)));

            if(timestampAttribute == null) {
                return records;
            } else {
                return records.apply("WithTimestamp", DataTypeTransform
                        .withTimestamp(DataType.AVRO, timestampAttribute, timestampDefault));
            }
        }

        private static class QueryDoFn extends DoFn<String, ReadStream> {

            private final String project;
            private final String query;
            private final String queryLocation;
            private final String dataset;
            private final String table;
            private final boolean createDataset;
            private final String kmsKey;

            public QueryDoFn(final String project,
                             final String query,
                             final String queryLocation,
                             final String dataset,
                             final String table,
                             final boolean createDataset,
                             final String kmsKey) {

                this.project = project;
                this.query = query;
                this.queryLocation = queryLocation;
                this.dataset = dataset;
                this.table = table;
                this.createDataset = createDataset;
                this.kmsKey = kmsKey;
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final Bigquery bigquery = BigQueryUtil.getBigquery();
                try {
                    TableReference destinationTable;
                    if(createDataset) {
                        bigquery.datasets().insert(project, new Dataset()
                                .setDatasetReference(new DatasetReference()
                                        .setProjectId(project)
                                        .setDatasetId(dataset))
                                .setDefaultTableExpirationMs(1000 * 60 * 60 * 24L)
                                .setLocation(queryLocation))
                                .execute();
                        destinationTable = new TableReference()
                                .setProjectId(project)
                                .setDatasetId(dataset)
                                .setTableId(table);
                    } else {
                        destinationTable = new TableReference()
                                .setProjectId(project)
                                .setDatasetId(dataset)
                                .setTableId(table);
                    }

                    final JobConfigurationQuery jobConfigurationQuery = new JobConfigurationQuery()
                            .setQuery(query)
                            .setDestinationTable(destinationTable)
                            .setAllowLargeResults(true)
                            .setCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED.name())
                            .setWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE.name())
                            .setPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE.name())
                            .setUseLegacySql(false);

                    if(kmsKey != null) {
                        jobConfigurationQuery.setDestinationEncryptionConfiguration(new EncryptionConfiguration().setKmsKeyName(kmsKey));
                    }

                    final Job queryJob = bigquery.jobs().insert(project, new Job()
                            .setConfiguration(new JobConfiguration().setQuery(jobConfigurationQuery)))
                            .execute();

                    final BackOff backOff = BackOffAdapter.toGcpBackOff(
                            FluentBackoff.DEFAULT
                                    .withMaxRetries(9)
                                    .withInitialBackoff(Duration.standardSeconds(1))
                                    .withMaxBackoff(Duration.standardMinutes(1))
                                    .backoff());

                    final Job resultJob = BigQueryUtil.pollJob(bigquery, queryJob.getJobReference(), Sleeper.DEFAULT, backOff);
                    if(!BigQueryUtil.isJobResultSucceeded(resultJob)) {
                        throw new RuntimeException(String.format("Query job %s failed, status: %s", "", resultJob.getStatus()));
                    }

                    final CreateReadSessionRequest request = CreateReadSessionRequest.newBuilder()
                            .setParent(String.format("projects/%s", project))
                            .setReadSession(ReadSession.newBuilder()
                                    .setTable(String.format("projects/%s/datasets/%s/tables/%s",
                                            destinationTable.getProjectId(), destinationTable.getDatasetId(), destinationTable.getTableId()))
                                    .setDataFormat(DataFormat.AVRO))
                            .setMaxStreamCount(0)
                            .build();

                    final ReadSession readSession;
                    try (BigQueryReadClient storageClient = BigQueryReadClient.create()) {
                        readSession = storageClient.createReadSession(request);
                    }

                    for(final ReadStream readStream : readSession.getStreamsList()) {
                        c.output(readStream);
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }

        private static class ReadDoFn extends DoFn<ReadStream, GenericRecord> {

            private final String schemaString;

            private transient Schema schema;
            private transient DatumReader<GenericRecord> datumReader;
            private transient BigQueryReadClient client;

            private transient BinaryDecoder decoder;
            private transient GenericRecord record;

            public ReadDoFn(final String schemaString) {
                this.schemaString = schemaString;
            }

            @Setup
            public void setup() throws IOException {
                this.schema = new Schema.Parser().parse(this.schemaString);
                this.datumReader = new GenericDatumReader<>(schema);
                this.client = BigQueryReadClient.create();
                this.decoder = null;
                this.record = null;
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                final ReadStream readStream = c.element();

                final ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder()
                        .setReadStream(readStream.getName())
                        .build();

                final ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
                for (final ReadRowsResponse response : stream) {
                    decoder = DecoderFactory.get()
                            .binaryDecoder(response.getAvroRows().getSerializedBinaryRows().toByteArray(), decoder);

                    while (!decoder.isEnd()) {
                        record = datumReader.read(record, decoder);
                        c.output(record);
                    }
                }

            }
        }

        private static class CleanDoFn extends DoFn<GenericRecord, Void> {

            private final String project;
            private final String dataset;
            private final String table;
            private final boolean deleteDataset;

            public CleanDoFn(final String project, final String dataset, final String table, final boolean deleteDataset) {
                this.project = project;
                this.dataset = dataset;
                this.table = table;
                this.deleteDataset = deleteDataset;
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {

                final Bigquery bigquery = BigQueryUtil.getBigquery();

                try {
                    bigquery.tables().delete(project, dataset, table).execute();
                } catch (IOException e) {

                }

                if(deleteDataset) {
                    try {
                        bigquery.datasets().delete(project, dataset).execute();
                    } catch (IOException e) {

                    }
                }
            }
        }

    }

    private static class BigQueryMicrobatchRead extends PTransform<PCollection<Long>, PCollection<GenericRecord>> {

        private Schema schema;

        private final String timestampAttribute;
        private final BigQuerySourceParameters parameters;

        public BigQueryMicrobatchRead(final SourceConfig config) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.parameters = new Gson().fromJson(config.getParameters(), BigQuerySourceParameters.class);
            validateParameters();
            setDefaultParameters();
        }

        private void validateParameters() {
            if (this.parameters == null) {
                throw new IllegalArgumentException("BigQuery SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if (parameters.getQuery() == null) {
                errorMessages.add("BigQuery source module[microbatch mode] parameters must contain query");
            }
            if (parameters.getQueryTempDataset() == null) {
                errorMessages.add("BigQuery source module[microbatch mode] parameters must contain queryTempDataset");
            }
            if (errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if (parameters.getQueryLocation() == null) {
                parameters.setQueryLocation("US");
            }
            //
            if(parameters.getIntervalSecond() == null) {
                parameters.setIntervalSecond(60);
            }
            if(parameters.getGapSecond() == null) {
                parameters.setGapSecond(30);
            }
            if(parameters.getMaxDurationMinute() == null) {
                parameters.setMaxDurationMinute(60);
            }
            if(parameters.getCatchupIntervalSecond() == null) {
                parameters.setCatchupIntervalSecond(parameters.getIntervalSecond());
            }
            if(parameters.getUseCheckpointAsStartDatetime() == null) {
                parameters.setUseCheckpointAsStartDatetime(false);
            }
        }

        public PCollection<GenericRecord> expand(final PCollection<Long> beat) {

            final String sampleQuery = MicrobatchQuery.createQuery(MicrobatchQuery.createTemplate(
                    parameters.getQuery()), Instant.now(), Instant.now().plus(1));

            final String projectId = beat.getPipeline().getOptions().as(GcpOptions.class).getProject();
            this.schema = BigQueryUtil.getAvroSchemaFromQuery(projectId, sampleQuery);

            return beat
                    .apply("MicrobatchQuery", MicrobatchQuery.of(
                            parameters.getQuery(),
                            parameters.getStartDatetime(),
                            parameters.getIntervalSecond(),
                            parameters.getGapSecond(),
                            parameters.getMaxDurationMinute(),
                            parameters.getOutputCheckpoint(),
                            parameters.getCatchupIntervalSecond(),
                            parameters.getUseCheckpointAsStartDatetime(),
                            new MicrobatchQueryDoFn(
                                    projectId,
                                    parameters.getQueryTempDataset(),
                                    parameters.getQueryLocation(),
                                    1000L,
                                    schema,
                                    timestampAttribute)))
                    .setCoder(AvroCoder.of(schema));
        }

        private static class MicrobatchQueryDoFn extends DoFn<KV<KV<Integer, KV<Long, Instant>>, String>, GenericRecord> {

            private static final Logger LOG = LoggerFactory.getLogger(MicrobatchQueryDoFn.class);

            private final String projectId;
            private final String dataset;
            private final String location;
            private final Long timeoutMs;
            private final String schemaString;
            private final String timestampAttribute;

            private transient Schema schema;
            private transient Bigquery bigquery;
            private transient DatasetReference datasetReference;

            private MicrobatchQueryDoFn(final String projectId,
                                        final String dataset,
                                        final String location,
                                        final Long timeoutMs,
                                        final Schema schema,
                                        final String timestampAttribute) {

                this.projectId = projectId;
                this.dataset = dataset;
                this.location = location;
                this.timeoutMs = timeoutMs;
                this.schemaString = schema.toString();
                this.timestampAttribute = timestampAttribute;
            }

            @Setup
            public void setup() {
                this.schema = new Schema.Parser().parse(schemaString);
                this.bigquery = BigQueryUtil.getBigquery();
                this.datasetReference = BigQueryUtil.getDatasetReference(dataset, projectId);
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {

                final Instant start = Instant.now();
                final String query = c.element().getValue();
                final QueryResponse response = this.bigquery.jobs()
                        .query(projectId, new QueryRequest()
                                .setQuery(query)
                                .setUseLegacySql(false)
                                .setTimeoutMs(timeoutMs)
                                .setLocation(location)
                                .setUseQueryCache(false)
                                .setMaxResults(5000L)
                                .setDefaultDataset(datasetReference))
                        .execute();

                final String jobId = response.getJobReference().getJobId();

                GetQueryResultsResponse queryResults = bigquery.jobs().getQueryResults(projectId, jobId).execute();
                while(!queryResults.getJobComplete()) {
                    Thread.sleep(1000L);
                    queryResults = bigquery.jobs().getQueryResults(projectId, jobId).execute();
                }

                if(queryResults.getTotalRows().longValue() == 0) {
                    final long time = Instant.now().getMillis() - start.getMillis();
                    LOG.info(String.format("Query [%s] result zero, took [%d] millisec to execute the query.",
                            query, time));
                    c.output(new TupleTag<>("checkpoint"), c.element().getKey());
                    return;
                }

                queryResults.getRows().stream()
                        .map(tableRow -> TableRowToRecordConverter.convertQueryResult(schema, tableRow))
                        .forEach(c::output);

                while(queryResults.getPageToken() != null) {
                    queryResults = bigquery.jobs()
                            .getQueryResults(projectId, jobId)
                            .setPageToken(queryResults.getPageToken())
                            .execute();
                    queryResults.getRows().stream()
                            .map(tableRow -> TableRowToRecordConverter.convertQueryResult(schema, tableRow))
                            .forEach(c::output);
                }

                final long time = Instant.now().getMillis() - start.getMillis();
                LOG.info(String.format("Query [%s] result num [%d], took [%d] millisec to execute the query.",
                        query, queryResults.getTotalRows().longValue(), time));

                c.output(new TupleTag<>("checkpoint"), c.element().getKey());
            }

            @Override
            public org.joda.time.Duration getAllowedTimestampSkew() {
                if (timestampAttribute != null) {
                    return org.joda.time.Duration.standardDays(365);
                } else {
                    return super.getAllowedTimestampSkew();
                }
            }

        }
    }

}
