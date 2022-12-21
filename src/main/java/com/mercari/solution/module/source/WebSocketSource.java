package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.JsonUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.JsonToRecordConverter;
import com.mercari.solution.util.converter.JsonToRowConverter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocketHandshakeException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class WebSocketSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketSource.class);

    private class WebSocketSourceParameters implements Serializable {

        private String endpoint;
        private JsonElement requests;
        private JsonElement heartbeatRequests;

        private Long intervalSeconds;
        private Long heartbeatIntervalSeconds;
        private Long checkIntervalSeconds;
        private String receivedTimestampField;
        private String eventtimeField;

        private Format format;
        private OutputType outputType;
        private Boolean ignoreError;
        private Boolean isArrayContent;
        private Long requestIntervalSeconds;
        private Boolean pruning;

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public JsonElement getRequests() {
            return requests;
        }

        public void setRequests(JsonElement requests) {
            this.requests = requests;
        }

        public JsonElement getHeartbeatRequests() {
            return heartbeatRequests;
        }

        public void setHeartbeatRequests(JsonElement heartbeatRequests) {
            this.heartbeatRequests = heartbeatRequests;
        }

        public Long getIntervalSeconds() {
            return intervalSeconds;
        }

        public void setIntervalSeconds(Long intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }

        public Long getHeartbeatIntervalSeconds() {
            return heartbeatIntervalSeconds;
        }

        public void setHeartbeatIntervalSeconds(Long heartbeatIntervalSeconds) {
            this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        }

        public Long getCheckIntervalSeconds() {
            return checkIntervalSeconds;
        }

        public void setCheckIntervalSeconds(Long checkIntervalSeconds) {
            this.checkIntervalSeconds = checkIntervalSeconds;
        }

        public String getReceivedTimestampField() {
            return receivedTimestampField;
        }

        public void setReceivedTimestampField(String receivedTimestampField) {
            this.receivedTimestampField = receivedTimestampField;
        }

        public String getEventtimeField() {
            return eventtimeField;
        }

        public void setEventtimeField(String eventtimeField) {
            this.eventtimeField = eventtimeField;
        }

        public Format getFormat() {
            return format;
        }

        public void setFormat(Format format) {
            this.format = format;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public void setOutputType(OutputType outputType) {
            this.outputType = outputType;
        }

        public Boolean getIgnoreError() {
            return ignoreError;
        }

        public void setIgnoreError(Boolean ignoreError) {
            this.ignoreError = ignoreError;
        }

        public Boolean getIsArrayContent() {
            return isArrayContent;
        }

        public void setIsArrayContent(Boolean isArrayContent) {
            this.isArrayContent = isArrayContent;
        }

        public Long getRequestIntervalSeconds() {
            return requestIntervalSeconds;
        }

        public void setRequestIntervalSeconds(Long requestIntervalSeconds) {
            this.requestIntervalSeconds = requestIntervalSeconds;
        }

        public Boolean getPruning() {
            return pruning;
        }

        public void setPruning(Boolean pruning) {
            this.pruning = pruning;
        }

        private void validateParameters(final PBegin begin) {

            if(!OptionUtil.isStreaming(begin.getPipeline().getOptions())) {
                throw new IllegalArgumentException("WebSocket source module only support streaming mode.");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.getEndpoint() == null) {
                errorMessages.add("WebSocket source module requires endpoint parameter");
            }
            // check optional parameters
            if(this.getIntervalSeconds() != null) {
                if(this.getIntervalSeconds() < 1) {
                    errorMessages.add("WebSocket source module intervalSeconds parameter must over zero");
                }
            }
            if(this.getHeartbeatIntervalSeconds() != null) {
                if(this.getHeartbeatIntervalSeconds() < 0) {
                    errorMessages.add("WebSocket source module heartbeatIntervalSeconds parameter must over zero");
                }
            }
            if(this.getCheckIntervalSeconds() != null) {
                if(this.getCheckIntervalSeconds() < 0) {
                    errorMessages.add("WebSocket source module checkIntervalSeconds parameter must over zero");
                }
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if(this.getFormat() == null) {
                this.setFormat(Format.json);
            }
            if(this.getIntervalSeconds() == null) {
                this.setIntervalSeconds(1L);
            }
            if(this.getHeartbeatIntervalSeconds() == null) {
                this.setHeartbeatIntervalSeconds(60L);
            }
            if(this.getCheckIntervalSeconds() == null) {
                this.setCheckIntervalSeconds(0L);
            }
            if(this.getOutputType() == null) {
                switch (this.getFormat()) {
                    case json: {
                        this.setOutputType(OutputType.row);
                        break;
                    }
                    default:
                        this.setOutputType(OutputType.row);
                        break;
                }
            }
            if(this.getIgnoreError() == null) {
                this.setIgnoreError(false);
            }
            if(this.getIsArrayContent() == null) {
                this.setIsArrayContent(false);
            }
            if(this.getRequestIntervalSeconds() == null) {
                this.setRequestIntervalSeconds(0L);
            }
            if(this.getPruning() == null) {
                this.setPruning(false);
            }
        }
    }

    public String getName() { return "websocket"; }

    private enum Format {
        json,
        text
    }

    private enum OutputType {
        row,
        avro,
        message
    }



    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        return stream(begin, config);
    }

    public static Map<String, FCollection<?>> stream(final PBegin begin, final SourceConfig config) {

        final WebSocketSourceParameters parameters = new Gson().fromJson(config.getParameters(), WebSocketSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("WebSocket source module parameters must not be empty!");
        }

        parameters.validateParameters(begin);
        parameters.setDefaultParameters();

        final List<SourceConfig.AdditionalOutput> additions = Optional
                .ofNullable(config.getAdditionalOutputs()).orElse(new ArrayList<>());
        final TupleTag<KV<Instant, String>> failuresTag = new TupleTag<>() {};

        switch (parameters.getFormat()) {
            case json: {
                switch (parameters.getOutputType()) {
                    case avro: {
                        final TupleTag<GenericRecord> outputTag = new TupleTag<>() {};
                        org.apache.avro.Schema outputSchema = SourceConfig.convertAvroSchema(config.getSchema());
                        if(parameters.getReceivedTimestampField() != null || parameters.getEventtimeField() != null) {
                            SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = AvroSchemaUtil
                                    .toBuilder(outputSchema, outputSchema.getNamespace(), null);
                            if(parameters.getReceivedTimestampField() != null) {
                                builder = builder
                                        .name(parameters.getReceivedTimestampField())
                                        .type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE)
                                        .noDefault();
                            }
                            if(parameters.getEventtimeField() != null) {
                                builder = builder
                                        .name(parameters.getEventtimeField())
                                        .type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE)
                                        .noDefault();
                            }
                            outputSchema = builder.endRecord();
                        }

                        final List<KV<TupleTag<GenericRecord>, SourceConfig.Output>> additionalOutputs = additions
                                .stream()
                                .map(a -> KV.of(new TupleTag<GenericRecord>(), a.toOutput()))
                                .collect(Collectors.toList());
                        final Map<String, String> additionalOutputSchemas = additions
                                .stream()
                                .collect(Collectors.toMap(
                                        SourceConfig.AdditionalOutput::getName,
                                        o -> {
                                            final org.apache.avro.Schema s = SourceConfig.convertAvroSchema(o.getSchema());
                                            if(parameters.getReceivedTimestampField() == null && parameters.getEventtimeField() == null) {
                                                return s.toString();
                                            }
                                            SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = AvroSchemaUtil
                                                    .toBuilder(s, s.getNamespace(), null);
                                            if(parameters.getReceivedTimestampField() != null) {
                                                builder = builder
                                                        .name(parameters.getReceivedTimestampField())
                                                        .type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE)
                                                        .noDefault();
                                            }
                                            if(parameters.getEventtimeField() != null) {
                                                builder = builder
                                                        .name(parameters.getEventtimeField())
                                                        .type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE)
                                                        .noDefault();
                                            }
                                            return builder.endRecord().toString();
                                        }));

                        final WebSocketStream<String, org.apache.avro.Schema, GenericRecord> stream = new WebSocketStream<>(
                                config.getName(), parameters,
                                outputTag, failuresTag,
                                outputSchema.toString(), additionalOutputSchemas,
                                AvroSchemaUtil::convertSchema, JsonToRecordConverter::convert,
                                AvroSchemaUtil::merge, (Instant timestamp) -> timestamp.getMillis() * 1000L,
                                additionalOutputs);
                        final PCollectionTuple outputs = begin.apply(config.getName(), stream);

                        final Map<String, FCollection<?>> collections = new HashMap<>();
                        final Coder<GenericRecord> outputCoder = AvroCoder.of(outputSchema);
                        collections.put(config.getName(), FCollection.of(config.getName(), outputs.get(outputTag).setCoder(outputCoder), DataType.AVRO, outputSchema));
                        for(final KV<TupleTag<GenericRecord>, SourceConfig.Output> kv : additionalOutputs) {
                            final String name = config.getName() + "." + kv.getValue().getName();
                            final org.apache.avro.Schema additionalOutputSchema = AvroSchemaUtil
                                    .convertSchema(additionalOutputSchemas.get(kv.getValue().getName()));
                            final Coder<GenericRecord> additionalOutputCoder = AvroCoder.of(additionalOutputSchema);
                            collections.put(name, FCollection.of(name, outputs.get(kv.getKey()).setCoder(additionalOutputCoder), DataType.AVRO, additionalOutputSchema));
                        }
                        return collections;
                    }
                    case row: {
                        final TupleTag<Row> outputTag = new TupleTag<>() {};
                        Schema outputSchema = SourceConfig.convertSchema(config.getSchema());
                        if(parameters.getReceivedTimestampField() != null || parameters.getEventtimeField() != null) {
                            Schema.Builder builder = RowSchemaUtil.toBuilder(outputSchema);
                            if(parameters.getReceivedTimestampField() != null) {
                                builder = builder.addField(
                                        parameters.getReceivedTimestampField(),
                                        org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true));
                            }
                            if(parameters.getEventtimeField() != null) {
                                builder = builder.addField(
                                        parameters.getEventtimeField(),
                                        org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true));
                            }
                            outputSchema = builder.build();
                        }

                        final List<KV<TupleTag<Row>, SourceConfig.Output>> additionalOutputs = additions
                                .stream()
                                .map(a -> KV.of(new TupleTag<Row>(), a.toOutput()))
                                .collect(Collectors.toList());
                        final Map<String, Schema> additionalOutputSchemas = additions
                                .stream()
                                .collect(Collectors.toMap(
                                        SourceConfig.AdditionalOutput::getName,
                                        o -> {
                                            final Schema s = SourceConfig.convertSchema(o.getSchema());
                                            if(parameters.getReceivedTimestampField() == null && parameters.getEventtimeField() == null) {
                                                return s;
                                            }
                                            Schema.Builder builder = RowSchemaUtil.toBuilder(s);
                                            if(parameters.getReceivedTimestampField() != null) {
                                                builder = builder.addField(
                                                        parameters.getReceivedTimestampField(),
                                                                org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true));
                                            }
                                            if(parameters.getEventtimeField() != null) {
                                                builder = builder.addField(
                                                        parameters.getEventtimeField(),
                                                        org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true));
                                            }
                                            return builder.build();
                                        }));

                        final WebSocketStream<Schema, Schema, Row> stream = new WebSocketStream<>(
                                config.getName(), parameters,
                                outputTag, failuresTag,
                                outputSchema, additionalOutputSchemas,
                                s -> s, JsonToRowConverter::convert,
                                RowSchemaUtil::merge, (Instant timestamp) -> timestamp,
                                additionalOutputs);
                        final PCollectionTuple outputs = begin.apply(config.getName(), stream);

                        final Map<String, FCollection<?>> collections = new HashMap<>();
                        final Coder<Row> outputCoder = RowCoder.of(outputSchema);
                        collections.put(config.getName(), FCollection.of(config.getName(), outputs.get(outputTag).setCoder(outputCoder), DataType.ROW, outputSchema));
                        for(final KV<TupleTag<Row>, SourceConfig.Output> kv : additionalOutputs) {
                            final String name = config.getName() + "." + kv.getValue().getName();
                            final Schema additionalOutputSchema = additionalOutputSchemas.get(kv.getValue().getName());
                            final Coder<Row> additionalOutputCoder = RowCoder.of(additionalOutputSchema);
                            collections.put(name, FCollection.of(name, outputs.get(kv.getKey()).setCoder(additionalOutputCoder), DataType.ROW, additionalOutputSchema));
                        }
                        return collections;
                    }
                    default:
                        throw new IllegalStateException("WebSocket source module does not support outputType: " + parameters.getOutputType());
                }
            }
            case text:
            default: {
                throw new IllegalStateException("WebSocket source module does not support format: " + parameters.getFormat());
            }
        }
    }

    public static class WebSocketStream<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PBegin, PCollectionTuple> {

        private final String name;
        private final String endpoint;
        private final Format format;
        private final List<String> requests;
        private final List<String> heartbeatRequests;
        private final Long intervalMillis;
        private final Long heartbeatIntervalMillis;
        private final Long checkIntervalMillis;
        private final String receivedTimestampField;
        private final String eventtimeField;
        private final Boolean ignoreError;
        private final Boolean isArrayContent;
        private final Long requestIntervalMillis;
        private final Boolean pruning;
        private final List<KV<TupleTag<T>, SourceConfig.Output>> additionalOutputs;

        private final TupleTag<T> outputTag;
        private final TupleTag<KV<Instant, String>> failuresTag;
        private final InputSchemaT inputSchema;
        private final Map<String, InputSchemaT> additionalOutputInputSchemas;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.JsonElementConverter<RuntimeSchemaT, T> jsonConverter;
        private final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter;
        private final TimestampConverter timestampConverter;

        private WebSocketStream(final String name,
                                final WebSocketSourceParameters parameters,
                                final TupleTag<T> outputTag, final TupleTag<KV<Instant, String>> failuresTag,
                                final InputSchemaT inputSchema, Map<String, InputSchemaT> additionalOutputInputSchemas,
                                final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                final SchemaUtil.JsonElementConverter<RuntimeSchemaT, T> jsonConverter,
                                final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter,
                                final TimestampConverter timestampConverter,
                                final List<KV<TupleTag<T>, SourceConfig.Output>> additionalOutputs) {

            this.name = name;
            this.endpoint = parameters.getEndpoint();
            this.format = parameters.getFormat();
            this.intervalMillis = parameters.getIntervalSeconds() * 1000L;
            this.heartbeatIntervalMillis = parameters.getHeartbeatIntervalSeconds() * 1000L;
            this.checkIntervalMillis = parameters.getCheckIntervalSeconds() * 1000L;
            this.receivedTimestampField = parameters.getReceivedTimestampField();
            this.eventtimeField = parameters.getEventtimeField();
            this.ignoreError = parameters.getIgnoreError();
            this.isArrayContent = parameters.getIsArrayContent();
            this.requestIntervalMillis = parameters.getRequestIntervalSeconds() * 1000L;
            this.pruning = parameters.getPruning();

            this.outputTag = outputTag;
            this.failuresTag = failuresTag;
            this.inputSchema = inputSchema;
            this.additionalOutputInputSchemas = additionalOutputInputSchemas;
            this.schemaConverter = schemaConverter;
            this.jsonConverter = jsonConverter;
            this.valuesSetter = valuesSetter;
            this.timestampConverter = timestampConverter;
            this.additionalOutputs = additionalOutputs;

            if(parameters.getRequests() != null && !parameters.getRequests().isJsonNull()) {
                final List<String> rs = new ArrayList<>();
                if(parameters.getRequests().isJsonObject()) {
                    rs.add(parameters.getRequests().toString());
                } else if(parameters.getRequests().isJsonArray()) {
                    for(final JsonElement element : parameters.getRequests().getAsJsonArray()) {
                        if(element.isJsonObject())  {
                            rs.add(element.toString());
                        } else if(element.isJsonPrimitive()) {
                            rs.add(element.getAsString());
                        }
                    }
                } else if(parameters.getRequests().isJsonPrimitive()) {
                    rs.add(parameters.getRequests().getAsString());
                }
                this.requests = rs;
            } else {
                this.requests = null;
            }

            if(parameters.getHeartbeatRequests() != null && !parameters.getHeartbeatRequests().isJsonNull()) {
                final List<String> rs = new ArrayList<>();
                if(parameters.getHeartbeatRequests().isJsonObject()) {
                    rs.add(parameters.getHeartbeatRequests().toString());
                } else if(parameters.getHeartbeatRequests().isJsonArray()) {
                    for(final JsonElement element : parameters.getHeartbeatRequests().getAsJsonArray()) {
                        if(element.isJsonObject())  {
                            rs.add(element.toString());
                        } else if(element.isJsonPrimitive()) {
                            rs.add(element.getAsString());
                        }
                    }
                } else if(parameters.getHeartbeatRequests().isJsonPrimitive()) {
                    rs.add(parameters.getHeartbeatRequests().getAsString());
                }
                this.heartbeatRequests = rs;
            } else {
                this.heartbeatRequests = null;
            }
        }

        public PCollectionTuple expand(final PBegin begin) {

            final PCollection<KV<String, Long>> beats = begin
                    .apply("GenerateSequence", GenerateSequence
                            .from(0)
                            .withRate(1, Duration.millis(intervalMillis)))
                    .apply("ToKV", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                            .via((beat) -> KV.of("", beat)));

            switch (format) {
                case json: {
                    TupleTagList tagList = TupleTagList.of(failuresTag);
                    for(final KV<TupleTag<T>, SourceConfig.Output> kv : additionalOutputs) {
                        tagList = tagList.and(kv.getKey());
                    }
                    return beats
                            .apply("ReceiveMessage", ParDo.of(new WebSocketDoFn(
                                    name, endpoint, requests,
                                    heartbeatRequests, heartbeatIntervalMillis, checkIntervalMillis, requestIntervalMillis, pruning)))
                            .setCoder(KvCoder.of(InstantCoder.of(), StringUtf8Coder.of()))
                            .apply("JsonToRecord", ParDo
                                    .of(new JsonConvertDoFn<>(
                                            name,
                                            failuresTag,
                                            inputSchema, additionalOutputInputSchemas,
                                            schemaConverter, jsonConverter,
                                            valuesSetter, timestampConverter,
                                            receivedTimestampField, eventtimeField, ignoreError, isArrayContent, additionalOutputs))
                                    .withOutputTags(outputTag, tagList));
                }
                default:
                    throw new IllegalArgumentException();
            }
        }

        private static class WebSocketDoFn extends DoFn<KV<String, Long>, KV<Instant, String>> {

            private static final String STATE_ID_BEAT = "beatState";
            private static final String STATE_ID_HEARTBEAT_EPOCH_MILLIS = "heartbeatPrevState";
            private static final String STATE_ID_CHECK_EPOCH_MILLIS = "checkPrevState";
            private static final String STATE_ID_INTERVAL_MESSAGE_COUNT = "intervalMessageCountState";

            @StateId(STATE_ID_BEAT)
            private final StateSpec<ValueState<Long>> beatStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_HEARTBEAT_EPOCH_MILLIS)
            private final StateSpec<ValueState<Long>> prevHeartbeatStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_CHECK_EPOCH_MILLIS)
            private final StateSpec<ValueState<Long>> prevCheckStateSpec = StateSpecs.value(BigEndianLongCoder.of());
            @StateId(STATE_ID_INTERVAL_MESSAGE_COUNT)
            private final StateSpec<ValueState<Integer>> intervalMessageCountStateSpec = StateSpecs.value(BigEndianIntegerCoder.of());

            private final String name;
            private final String endpoint;
            private final List<String> requests;
            private final List<String> heartbeatRequests;
            private final Long heartbeatIntervalMillis;
            private final Long checkIntervalMillis;
            private final Long requestIntervalMillis;
            private final Boolean pruning;

            private transient java.net.http.WebSocket socket;
            private transient Listener listener;

            WebSocketDoFn(final String name,
                          final String endpoint,
                          final List<String> requests,
                          final List<String> heartbeatRequests,
                          final Long heartbeatIntervalMillis,
                          final Long checkIntervalMillis,
                          final Long requestIntervalMillis,
                          final Boolean pruning) {

                this.name = name;
                this.endpoint = endpoint;
                this.requests = requests;
                this.heartbeatRequests = heartbeatRequests;
                this.heartbeatIntervalMillis = heartbeatIntervalMillis;
                this.checkIntervalMillis = checkIntervalMillis;
                this.requestIntervalMillis = requestIntervalMillis;
                this.pruning = pruning;
            }

            private void connect() throws InterruptedException, ExecutionException {
                this.listener = new Listener(name, requests);
                final java.net.http.WebSocket.Builder wsb = HttpClient.newHttpClient().newWebSocketBuilder();
                final CompletableFuture<java.net.http.WebSocket> comp = wsb.buildAsync(URI.create(endpoint), listener);
                this.socket = comp.get();
                LOG.info("WebSocket[" + name + "] connected");
            }

            @Setup
            public void setup() throws InterruptedException, ExecutionException {
                LOG.info("WebSocket[" + name + "] setup");
                try {
                    connect();
                } catch (final ExecutionException e) {
                    final String message;
                    if(e.getCause() instanceof WebSocketHandshakeException) {
                        final WebSocketHandshakeException hse = (WebSocketHandshakeException) e.getCause();
                        message = "WebSocket[" + name + "] setup failed to connect. WebSocketHandshakeException.statusCode: " + hse.getResponse().statusCode() + ", body: "  + hse.getResponse().body();
                    } else {
                        message = "WebSocket[" + name + "] setup failed to connect. cause: " + e.getCause() + ", message: "  + e.getMessage();
                    }
                    LOG.error(message);
                    throw new IllegalStateException(message, e);
                }
            }

            @Teardown
            public void teardown() throws InterruptedException, ExecutionException {
                LOG.info("WebSocket[" + name + "] teardown");
                final CompletableFuture<java.net.http.WebSocket> comp = socket.sendClose(java.net.http.WebSocket.NORMAL_CLOSURE, "");
                this.socket = comp.get();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATE_ID_BEAT) ValueState<Long> beatState,
                                       final @StateId(STATE_ID_HEARTBEAT_EPOCH_MILLIS) ValueState<Long> prevHeartbeatEpochMillisState,
                                       final @StateId(STATE_ID_CHECK_EPOCH_MILLIS) ValueState<Long> prevCheckEpochMillisState,
                                       final @StateId(STATE_ID_INTERVAL_MESSAGE_COUNT) @AlwaysFetched ValueState<Integer> intervalMessageCountState)
                    throws InterruptedException, ExecutionException {

                int messageCount = Optional.ofNullable(intervalMessageCountState.read()).orElse(0);
                synchronized (listener) {
                    final List<KV<Instant, String>> messages = listener.getMessages();
                    messageCount += messages.size();
                    if(pruning) {
                        if(messages.size() > 0) {
                            final KV<Instant, String> message = messages.get(messages.size() - 1);
                            c.output(message);
                        }
                    } else {
                        for(final KV<Instant, String> message : messages) {
                            c.output(message);
                        }
                    }
                    listener.clearMessages();
                }

                if(this.listener.isClosed()) {
                    LOG.warn("WebSocket[" + name + "] is closed. Start connection");
                    connect();
                } else if(this.socket.isOutputClosed()) {
                    LOG.warn("WebSocket[" + name + "] output is closed. Start connection");
                    connect();
                } else if(this.socket.isInputClosed()) {
                    LOG.warn("WebSocket[" + name + "] input is closed. Start connection");
                    connect();
                } else {
                    if(this.heartbeatRequests != null && this.heartbeatRequests.size() > 0) {
                        final long prevEpochMillis = Optional.ofNullable(prevHeartbeatEpochMillisState.read()).orElse(0L);
                        final long nextEpochMillis = Instant.now().getMillis();
                        if(nextEpochMillis - prevEpochMillis > this.heartbeatIntervalMillis) {
                            for (final String request : this.heartbeatRequests) {
                                this.socket.sendText(request, true);
                            }
                            prevHeartbeatEpochMillisState.write(nextEpochMillis);
                        }
                    }
                    if(this.checkIntervalMillis > 0L) {
                        final long prevEpochMillis = Optional.ofNullable(prevCheckEpochMillisState.read()).orElseGet(() -> Instant.now().getMillis());
                        final long nextEpochMillis = Instant.now().getMillis();
                        if(nextEpochMillis - prevEpochMillis > this.checkIntervalMillis) {
                            if (messageCount == 0) {
                                LOG.warn("WebSocket[" + name + "] no message in fixed millis: " + heartbeatIntervalMillis + ". Start connection");
                                connect();
                            }
                            messageCount = 0;
                            prevCheckEpochMillisState.write(nextEpochMillis);
                        }
                    }
                }

                beatState.write(c.element().getValue());
                intervalMessageCountState.write(messageCount);
            }

            private class Listener implements java.net.http.WebSocket.Listener {

                private final String name;
                private StringBuilder stringBuffer;
                private ByteBuffer byteBuffer;
                private boolean isClosed = true;
                private final List<String> requests;

                private final List<KV<Instant, String>> messages;

                Listener(final String name, final List<String> requests) {
                    this.name = name;
                    this.requests = requests;
                    this.messages = Collections.synchronizedList(new ArrayList<>());
                }

                @Override
                public void onOpen(java.net.http.WebSocket webSocket) {
                    this.stringBuffer = new StringBuilder();
                    this.byteBuffer = ByteBuffer.allocate(100000);
                    java.net.http.WebSocket.Listener.super.onOpen(webSocket);
                    LOG.info("WebSocket[" + name + "] onOpen");
                    this.isClosed = false;
                    if(requests != null) {
                        final Map<String, Object> data = new HashMap<>();
                        for(final String request : requests) {
                            final String body = TemplateUtil.executeStrictTemplate(request, data);
                            webSocket.sendText(body, true);
                            LOG.info("WebSocket[" + name + "] send message: " + body);
                            if(requestIntervalMillis > 0) {
                                try {
                                    Thread.sleep(requestIntervalMillis);
                                } catch (InterruptedException e) {
                                    LOG.warn("WebSocket[" + name + "] throws exception: " + e);
                                }
                            }
                        }
                    }
                }

                @Override
                public CompletionStage<?> onText(java.net.http.WebSocket webSocket, CharSequence data, boolean last) {
                    this.stringBuffer.append(data);
                    if(last) {
                        final String message = this.stringBuffer.toString();
                        this.stringBuffer.setLength(0);
                        this.messages.add(KV.of(Instant.now(), message));
                    }
                    return java.net.http.WebSocket.Listener.super.onText(webSocket, data, last);
                }

                @Override
                public CompletionStage<?> onBinary(java.net.http.WebSocket webSocket, ByteBuffer data, boolean last) {
                    this.byteBuffer.put(data);
                    if(last) {
                        final ByteBuffer message = this.byteBuffer.asReadOnlyBuffer();
                        this.byteBuffer.clear();
                        this.messages.add(KV.of(Instant.now(), message.toString()));
                    }
                    return java.net.http.WebSocket.Listener.super.onBinary(webSocket, data, last);
                }

                @Override
                public CompletionStage<?> onPing(java.net.http.WebSocket webSocket, ByteBuffer message) {
                    LOG.debug("WebSocket[" + name + "] onPing: " + message);
                    return java.net.http.WebSocket.Listener.super.onPing(webSocket, message);
                }

                @Override
                public CompletionStage<?> onPong(java.net.http.WebSocket webSocket, ByteBuffer message) {
                    LOG.debug("WebSocket[" + name + "] onPong: " + message);
                    return java.net.http.WebSocket.Listener.super.onPong(webSocket, message);
                }

                @Override
                public CompletionStage<?> onClose(java.net.http.WebSocket webSocket, int statusCode, String reason) {
                    LOG.info("WebSocket[" + name + "] onClosed. statusCode: " + statusCode + ", reason: " + reason);
                    this.isClosed = true;
                    return java.net.http.WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                }

                @Override
                public void onError(java.net.http.WebSocket webSocket, Throwable error) {
                    final String message = "WebSocket[" + name + "] onError. cause: " + error.getMessage();
                    LOG.error(message);
                    this.isClosed = true;
                    java.net.http.WebSocket.Listener.super.onError(webSocket, error);
                }
                public List<KV<Instant, String>> getMessages() {
                    return new ArrayList<>(this.messages);
                }

                public void clearMessages() {
                    this.messages.clear();
                }

                public boolean isClosed() {
                    return this.isClosed;
                }
            }

        }

        private static class JsonConvertDoFn<InputSchemaT, RuntimeSchemaT, T> extends DoFn<KV<Instant, String>, T> {

            private final String name;
            private final TupleTag<KV<Instant, String>> failuresTag;
            private final InputSchemaT inputSchema;
            private final Map<String, InputSchemaT> additionalOutputInputSchemas;
            private final String receivedTimestampField;
            private final String eventtimeField;
            private final Boolean ignoreError;
            private final List<KV<TupleTag<T>, SourceConfig.Output>> additionalOutputs;
            private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.JsonElementConverter<RuntimeSchemaT, T> jsonConverter;
            private final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter;
            private final TimestampConverter timestampConverter;
            private final Boolean isArrayContent;


            private transient Gson gson;
            private transient RuntimeSchemaT runtimeSchema;
            private transient Map<String, RuntimeSchemaT> additionalOutputSchemas;
            private transient Map<String, Filter.ConditionNode> additionalOutputConditions;

            JsonConvertDoFn(final String name,
                            final TupleTag<KV<Instant, String>> failuresTag,
                            final InputSchemaT inputSchema,
                            final Map<String, InputSchemaT> additionalOutputInputSchemas,
                            final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                            final SchemaUtil.JsonElementConverter<RuntimeSchemaT, T> jsonConverter,
                            final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter,
                            final TimestampConverter timestampConverter,
                            final String receivedTimestampField,
                            final String eventtimeField,
                            final Boolean ignoreError,
                            final Boolean isArrayContent,
                            final List<KV<TupleTag<T>, SourceConfig.Output>> additionalOutputs) {

                this.name = name;
                this.failuresTag = failuresTag;
                this.inputSchema = inputSchema;
                this.additionalOutputInputSchemas = additionalOutputInputSchemas;
                this.schemaConverter = schemaConverter;
                this.jsonConverter = jsonConverter;
                this.valuesSetter = valuesSetter;
                this.timestampConverter = timestampConverter;
                this.receivedTimestampField = receivedTimestampField;
                this.eventtimeField = eventtimeField;
                this.ignoreError = ignoreError;
                this.isArrayContent = isArrayContent;
                this.additionalOutputs = additionalOutputs;
            }

            @Setup
            public void setup() {
                this.gson = new Gson();
                this.runtimeSchema = schemaConverter.convert(inputSchema);
                this.additionalOutputSchemas = this.additionalOutputInputSchemas
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> schemaConverter.convert(e.getValue())));
                this.additionalOutputConditions = this.additionalOutputs
                        .stream()
                        .collect(Collectors.toMap(
                                o -> o.getValue().getName(),
                                o -> Filter.parse(this.gson.fromJson(o.getValue().getConditions(), JsonElement.class))));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Instant receivedAt = c.element().getKey();
                final String json = c.element().getValue();
                if("pong".equals(json)) {
                    return;
                }
                try {
                    final List<T> results = convert(runtimeSchema, json, receivedAt, c.timestamp());
                    for(final T record : results) {
                        c.output(record);
                    }
                } catch (final Exception e) {
                    for(final KV<TupleTag<T>, SourceConfig.Output> kv : additionalOutputs) {
                        final Filter.ConditionNode condition = additionalOutputConditions.get(kv.getValue().getName());
                        if(!Filter.filter(json, JsonUtil::getJsonPathValue, condition)) {
                            LOG.debug("Websocket input: " + name + " not matched condition: " + condition);
                            continue;
                        }
                        final RuntimeSchemaT additionalOutputSchema = this.additionalOutputSchemas.get(kv.getValue().getName());
                        if(additionalOutputSchema == null) {
                            throw new IllegalStateException("Websocket input: " + name + " not found additionalOutputSchema for: " + kv.getValue().getName() + " in: " + this.additionalOutputSchemas);
                        }
                        try {
                            final List<T> results = convert(additionalOutputSchema, json, receivedAt, c.timestamp());
                            for(final T record : results) {
                                c.output(kv.getKey(), record);
                            }
                            return;
                        } catch (final RuntimeException ee) {
                            LOG.error("Websocket input: " + name + " not match condition: " + kv.getValue().toString() + " with json: " + json + ", cause: " + ee.getMessage());
                        }
                    }

                    c.output(failuresTag, c.element());
                    final String message = "Websocket input: " + name + " failed to parse json: " + json + " cause: " + e.getMessage() + ", with schema: " + runtimeSchema;
                    LOG.error(message);
                    if(this.ignoreError) {
                        return;
                    }
                    throw new IllegalStateException(message, e);
                }
            }

            private List<T> convert(final RuntimeSchemaT schema,
                                    final String json,
                                    final Instant receivedAt,
                                    final Instant eventtime) {

                final List<T> results = new ArrayList<>();
                final JsonElement element = gson.fromJson(json, JsonElement.class);

                if(element.isJsonNull()) {
                    return results;
                }

                if(isArrayContent && element.isJsonArray()) {
                    for (final JsonElement elementArray : element.getAsJsonArray()) {
                        final T record = jsonConverter.convert(schema, elementArray);
                        if (receivedTimestampField == null && eventtimeField == null) {
                            results.add(record);
                        } else {
                            final Map<String, Object> values = new HashMap<>();
                            if(receivedTimestampField != null) {
                                values.put(receivedTimestampField, timestampConverter.convertTimestamp(receivedAt));
                            }
                            if(eventtimeField != null) {
                                values.put(eventtimeField, timestampConverter.convertTimestamp(eventtime));
                            }
                            final T recordWithReceivedAt = valuesSetter.setValues(schema, record, values);
                            results.add(recordWithReceivedAt);
                        }
                    }
                } else if(element.isJsonObject() || element.isJsonArray()) {
                    final T record = jsonConverter.convert(schema, element);
                    if(receivedTimestampField == null && eventtimeField == null) {
                        results.add(record);
                    } else {
                        final Map<String, Object> values = new HashMap<>();
                        if(receivedTimestampField != null) {
                            values.put(receivedTimestampField, timestampConverter.convertTimestamp(receivedAt));
                        }
                        if(eventtimeField != null) {
                            values.put(eventtimeField, timestampConverter.convertTimestamp(eventtime));
                        }
                        final T recordWithReceivedAt = valuesSetter.setValues(schema, record, values);
                        results.add(recordWithReceivedAt);
                    }
                } else {
                    throw new IllegalStateException("Illegal input json: " + element);
                }
                return results;
            }

        }
    }

    private interface TimestampConverter extends Serializable {
        Object convertTimestamp(Instant timestamp);
    }

}
