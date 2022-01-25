package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.JsonToRecordConverter;
import com.mercari.solution.util.converter.JsonToRowConverter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import okhttp3.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;


public class WebSocketSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketSource.class);

    private class WebSocketSourceParameters implements Serializable {

        private String endpoint;
        private JsonElement requests;

        private Long intervalMillis;
        private String receivedTimestampField;

        private Format format;
        private OutputType outputType;

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

        public Long getIntervalMillis() {
            return intervalMillis;
        }

        public void setIntervalMillis(Long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }

        public String getReceivedTimestampField() {
            return receivedTimestampField;
        }

        public void setReceivedTimestampField(String receivedTimestampField) {
            this.receivedTimestampField = receivedTimestampField;
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
        return Collections.singletonMap(config.getName(), stream(begin, config));
    }

    public static FCollection<?> stream(final PBegin begin, final SourceConfig config) {

        final WebSocketSourceParameters parameters = new Gson().fromJson(config.getParameters(), WebSocketSourceParameters.class);
        validateParameters(begin, parameters);
        setDefaultParameters(parameters);

        switch (parameters.getFormat()) {
            case json: {
                switch (parameters.getOutputType()) {
                    case avro: {
                        final WebSocketStream<GenericRecord> stream = new WebSocketStream<>(config, parameters);
                        final PCollection<GenericRecord> output = begin.apply(config.getName(), stream);
                        org.apache.avro.Schema avroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                        if(parameters.getReceivedTimestampField() != null) {
                            avroSchema = AvroSchemaUtil.toBuilder(avroSchema, avroSchema.getNamespace(), null)
                                    .name(parameters.getReceivedTimestampField()).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                                    .endRecord();
                        }
                        return FCollection.of(config.getName(), output, DataType.AVRO, avroSchema);
                    }
                    case row: {
                        final WebSocketStream<Row> stream = new WebSocketStream<>(config, parameters);
                        final PCollection<Row> output = begin.apply(config.getName(), stream);
                        Schema rowSchema = SourceConfig.convertSchema(config.getSchema());
                        if(parameters.getReceivedTimestampField() != null) {
                            rowSchema = RowSchemaUtil.toBuilder(rowSchema)
                                    .addNullableField(parameters.getReceivedTimestampField(), org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME)
                                    .build();
                        }
                        return FCollection.of(config.getName(), output, DataType.ROW, rowSchema);
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

    private static void validateParameters(final PBegin begin, final WebSocketSourceParameters parameters) {

        if(!begin.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming()) {
            throw new IllegalArgumentException("WebSocket source module only support streaming mode.");
        }

        if(parameters == null) {
            throw new IllegalArgumentException("WebSocket source module parameters must not be empty!");
        }

        // check required parameters filled
        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getEndpoint() == null) {
            errorMessages.add("WebSocket source module requires endpoint parameter");
        }
        // check optional parameters
        if(parameters.getRequests() != null) {
            if(parameters.getRequests().isJsonNull()) {
                errorMessages.add("WebSocket source module requests parameter must be not null. Set JsonObject or JsonArray");
            } else if(parameters.getRequests().isJsonPrimitive()) {
                errorMessages.add("WebSocket source module requests parameter must be not primitive. Set JsonObject or JsonArray");
            }
        }
        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
        }
    }

    private static void setDefaultParameters(final WebSocketSourceParameters parameters) {
        if(parameters.getFormat() == null) {
            parameters.setFormat(Format.json);
        }
        if(parameters.getIntervalMillis() == null) {
            parameters.setIntervalMillis(1000L);
        }
        if(parameters.getOutputType() == null) {
            switch (parameters.getFormat()) {
                case json: {
                    parameters.setOutputType(OutputType.row);
                    break;
                }
                default:
                    parameters.setOutputType(OutputType.row);
                    break;
            }
        }
    }

    public static class WebSocketStream<T> extends PTransform<PBegin, PCollection<T>> {

        private final SourceConfig.InputSchema schema;

        private final String endpoint;
        private final Format format;
        private final OutputType outputType;
        private final List<String> requests;
        private final Long intervalMillis;
        private final String receivedTimestampField;

        private WebSocketStream(final SourceConfig config, final WebSocketSourceParameters parameters) {
            this.schema = config.getSchema();
            this.endpoint = parameters.getEndpoint();
            this.format = parameters.getFormat();
            this.outputType = parameters.getOutputType();
            this.intervalMillis = parameters.getIntervalMillis();
            this.receivedTimestampField = parameters.getReceivedTimestampField();

            if(parameters.getRequests() != null) {
                final List<String> rs = new ArrayList<>();
                if(parameters.getRequests().isJsonObject()) {
                    rs.add(parameters.getRequests().toString());
                } else if(parameters.getRequests().isJsonArray()) {
                    for(final JsonElement element : parameters.getRequests().getAsJsonArray()) {
                        if(element.isJsonObject())  {
                            rs.add(element.toString());
                        }
                    }
                }
                this.requests = rs;
            } else {
                this.requests = null;
            }
        }

        public PCollection<T> expand(final PBegin begin) {

            final PCollection<KV<String, Long>> beats = begin
                    .apply("GenerateSequence", GenerateSequence
                            .from(0)
                            .withRate(1, Duration.millis(intervalMillis)))
                    .apply("ToKV", MapElements
                            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                            .via((beat) -> KV.of("", beat)));

            final TupleTag<KV<Instant, String>> failuresTag = new TupleTag<>() {};
            final PCollection<T> messages;
            final PCollection<KV<Instant, String>> failures;

            switch (format) {
                case json: {
                    final PCollection<KV<Instant, String>> jsons = beats
                            .apply("ReceiveMessage", ParDo.of(new WebSocketDoFn(endpoint, requests)))
                            .setCoder(KvCoder.of(InstantCoder.of(), StringUtf8Coder.of()));
                    switch (outputType) {
                        case avro: {
                            final TupleTag<GenericRecord> outputAvroTag = new TupleTag<>() {};
                            org.apache.avro.Schema avroSchema = SourceConfig.convertAvroSchema(schema);
                            if(receivedTimestampField != null) {
                                avroSchema = AvroSchemaUtil.toBuilder(avroSchema, avroSchema.getNamespace(), null)
                                        .name(receivedTimestampField).type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                                        .endRecord();
                            }
                            final PCollectionTuple tuple = jsons
                                    .apply("JsonToRecord", ParDo
                                            .of(new JsonToRecordDoFn(avroSchema.toString(), failuresTag, receivedTimestampField))
                                            .withOutputTags(outputAvroTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputAvroTag)
                                    .setCoder(AvroCoder.of(avroSchema));
                            failures = tuple.get(failuresTag);
                            break;
                        }
                        case row: {
                            final TupleTag<Row> outputRowTag = new TupleTag<>() {};
                            org.apache.beam.sdk.schemas.Schema rowSchema = SourceConfig.convertSchema(schema);
                            if(receivedTimestampField != null) {
                                rowSchema = RowSchemaUtil.toBuilder(rowSchema)
                                        .addNullableField(receivedTimestampField, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME)
                                        .build();
                            }
                            final PCollectionTuple tuple = jsons
                                    .apply("JsonToRow", ParDo
                                            .of(new JsonToRowDoFn(rowSchema, failuresTag, receivedTimestampField))
                                            .withOutputTags(outputRowTag, TupleTagList.of(failuresTag)));
                            messages = (PCollection<T>) tuple.get(outputRowTag)
                                    .setCoder(RowCoder.of(rowSchema));
                            failures = tuple.get(failuresTag);
                            break;
                        }
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException();
            }

            return messages;
        }
    }

    private static class WebSocketDoFn extends DoFn<KV<String, Long>, KV<Instant, String>> {

        @StateId("state")
        private final StateSpec<ValueState<Long>> stateSpec = StateSpecs.value(BigEndianLongCoder.of());

        private final String endpoint;
        private final List<String> requests;

        private transient WebSocket socket;
        private transient Listener listener;

        WebSocketDoFn(final String endpoint,
                      final List<String> requests) {
            this.endpoint = endpoint;
            this.requests = requests;
        }

        @Setup
        public void setup() throws URISyntaxException {
            this.listener = new WebSocketDoFn.Listener(requests);
            LOG.info("setup");
            final Request request = new Request.Builder()
                    .url(endpoint)
                    .get()
                    .build();
            this.socket = new OkHttpClient().newWebSocket(request, this.listener);
        }

        @Teardown
        public void teardown() {
            LOG.info("teardown");
            socket.close(0, "close");
        }

        @ProcessElement
        public void processElement(final ProcessContext c,
                                   final @StateId("state") ValueState<Long> state) {

            synchronized (listener) {
                for(final KV<Instant, String> message : listener.getMessages()) {
                    //LOG.info("from message: " + message.getValue());
                    c.output(message);
                }
                listener.clearMessages();
            }
            state.write(c.element().getValue());
        }

        private static class Listener extends WebSocketListener {

            private final List<String> requests;

            private final List<KV<Instant, String>> messages;

            Listener(final List<String> requests) {
                this.requests = requests;
                this.messages = Collections.synchronizedList(new ArrayList<>());
            }

            @Override
            public void onOpen(okhttp3.WebSocket webSocket, Response response) {
                LOG.info("socket open");
                super.onOpen(webSocket, response);
                if(requests != null) {
                    final Map<String, Object> data = new HashMap<>();
                    for(final String request : requests) {
                        final String body = TemplateUtil.executeStrictTemplate(request, data);
                        webSocket.send(body);
                    }
                }
            }

            @Override
            public void onFailure(okhttp3.WebSocket webSocket, Throwable t, @Nullable Response response) {
                final String message = "socket failure cause: " + t.getMessage() + ". response: " + (response == null ? "" : response.message());
                LOG.error(message);
                super.onFailure(webSocket, t, response);
                throw new IllegalStateException(message);
            }

            @Override
            public void onMessage(okhttp3.WebSocket webSocket, String text) {
                super.onMessage(webSocket, text);
                this.messages.add(KV.of(Instant.now(), text));
            }

            @Override
            public void onClosed(okhttp3.WebSocket webSocket, int code, String reason) {
                LOG.info("socket closed. code: " + code + ", reason: " + reason);
                super.onClosed(webSocket, code, reason);
            }

            @Override
            public void onClosing(okhttp3.WebSocket webSocket, int code, String reason) {
                LOG.info("socket closing. code: " + code + ", reason: " + reason);
                super.onClosing(webSocket, code, reason);
            }

            public List<KV<Instant, String>> getMessages() {
                return new ArrayList<>(this.messages);
            }

            public void clearMessages() {
                this.messages.clear();
            }
        }

    }

    private static class JsonToRowDoFn extends DoFn<KV<Instant, String>, Row> {

        private final TupleTag<KV<Instant, String>> failuresTag;
        private final org.apache.beam.sdk.schemas.Schema schema;
        private final String receivedTimestampField;

        private transient Gson gson;

        JsonToRowDoFn(final Schema schema,
                      final TupleTag<KV<Instant, String>> failuresTag,
                      final String receivedTimestampField) {

            this.schema = schema;
            this.failuresTag = failuresTag;
            this.receivedTimestampField = receivedTimestampField;
        }

        @Setup
        public void setup() {
            this.gson = new Gson();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Instant reveivedAt = c.element().getKey();
            final String json = c.element().getValue();
            try {
                final JsonElement element = gson.fromJson(json, JsonElement.class);
                if(element.isJsonObject()) {
                    final Row row = JsonToRowConverter.convert(schema, element.getAsJsonObject());
                    if(receivedTimestampField == null) {
                        c.output(row);
                    } else {
                        final Row rowWithTimestamp = withTimestamp(row, receivedTimestampField, reveivedAt);
                        c.output(rowWithTimestamp);
                    }
                } else if(element.isJsonArray()) {
                    for(final JsonElement elementArray : element.getAsJsonArray()) {
                        final Row row = JsonToRowConverter.convert(schema, elementArray.getAsJsonObject());
                        if(receivedTimestampField == null) {
                            c.output(row);
                        } else {
                            final Row rowWithTimestamp = withTimestamp(row, receivedTimestampField, reveivedAt);
                            c.output(rowWithTimestamp);
                        }                    }
                } else {
                    throw new IllegalStateException("Illegal input json: " + json);
                }

            } catch (Exception e) {
                c.output(failuresTag, c.element());
                LOG.error("Failed to parse json: " + json + ", cause: " + e.getMessage());
                throw new IllegalStateException("Failed to parse json: " + json);
            }
        }

        private Row withTimestamp(Row row, String field, Instant timestamp) {
            final Row.FieldValueBuilder builder = RowSchemaUtil.toBuilder(schema, row);
            builder.withFieldValue(field, timestamp);
            return builder.build();
        }

    }

    private static class JsonToRecordDoFn extends DoFn<KV<Instant, String>, GenericRecord> {

        private final TupleTag<KV<Instant, String>> failuresTag;
        private final String schemaString;
        private final String receivedTimestampField;

        private transient Gson gson;
        private transient org.apache.avro.Schema schema;

        JsonToRecordDoFn(final String schemaString,
                         final TupleTag<KV<Instant, String>> failuresTag,
                         final String receivedTimestampField) {

            this.schemaString = schemaString;
            this.failuresTag = failuresTag;
            this.receivedTimestampField = receivedTimestampField;
        }

        @Setup
        public void setup() {
            this.gson = new Gson();
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Instant reveivedAt = c.element().getKey();
            final String json = c.element().getValue();
            try {
                final JsonElement element = gson.fromJson(json, JsonElement.class);
                if(element.isJsonObject()) {
                    final GenericRecord record = JsonToRecordConverter.convert(schema, element.getAsJsonObject());
                    if(receivedTimestampField == null) {
                        c.output(record);
                    } else {
                        final GenericRecord recordWithRecievedAt = withTimestamp(record, receivedTimestampField, reveivedAt);
                        c.output(recordWithRecievedAt);
                    }
                } else if(element.isJsonArray()) {
                    for(final JsonElement elementArray : element.getAsJsonArray()) {
                        final GenericRecord record = JsonToRecordConverter.convert(schema, elementArray.getAsJsonObject());
                        if(receivedTimestampField == null) {
                            c.output(record);
                        } else {
                            final GenericRecord recordWithRecievedAt = withTimestamp(record, receivedTimestampField, reveivedAt);
                            c.output(recordWithRecievedAt);
                        }
                    }
                } else {
                    throw new IllegalStateException("Illegal input json: " + json);
                }
            } catch (Exception e) {
                c.output(failuresTag, c.element());
                LOG.error("Failed to parse json: " + json);
                throw new IllegalStateException("Failed to parse json: " + json);
            }
        }

        private GenericRecord withTimestamp(GenericRecord record, String fieldName, Instant timestamp) {
            return AvroSchemaUtil.toBuilder(schema, record)
                    .set(fieldName, timestamp.getMillis() * 1000L)
                    .build();
        }

    }

}
