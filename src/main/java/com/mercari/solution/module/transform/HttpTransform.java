package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.JsonToRecordConverter;
import com.mercari.solution.util.converter.JsonToRowConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.io.requestresponse.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class HttpTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransform.class);

    public static class HttpTransformParameters implements Serializable {

        private RequestParameters request;
        private ResponseParameters response;
        private RetryParameters retry;
        private Integer timeoutSecond;
        private JsonArray select;
        private JsonElement filter;

        private DataType outputType;
        private Boolean failFast;

        public static HttpTransformParameters of(
                final JsonElement jsonElement,
                final String name,
                final PInput input) {

            final HttpTransformParameters parameters = new Gson().fromJson(jsonElement, HttpTransformParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("HttpTransformParameters config parameters must not be empty!");
            }

            parameters.validate(name);
            parameters.setDefaults(input);

            return parameters;
        }

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.request == null) {
                errorMessages.add("http transform module[" + name + "].request must not be null.");
            } else {
                errorMessages.addAll(this.request.validate(name));
            }

            if(this.response == null) {
                errorMessages.add("http transform module[" + name + "].response must not be null.");
            } else {
                errorMessages.addAll(this.response.validate(name));
            }

            if(this.retry != null) {
                errorMessages.addAll(this.retry.validate(name));
            }

            return errorMessages;
        }

        public void setDefaults(PInput input) {
            this.request.setDefaults();
            this.response.setDefaults();
            if(this.retry == null) {
                this.retry = new RetryParameters();
            }
            this.retry.setDefaults();
            if(this.timeoutSecond == null) {
                this.timeoutSecond = 30;
            }
            if(failFast == null) {
                this.failFast = false;
            }
            if(outputType == null) {
                this.outputType = OptionUtil.isStreaming(input) ? DataType.ROW : DataType.AVRO;
            }
        }

        private static class RequestParameters implements Serializable {

            private String endpoint;
            private String method;
            private Map<String,String> params;
            private String body;
            private Map<String, String> headers;

            public List<String> validate(String name) {
                final List<String> errorMessages = new ArrayList<>();

                if(this.endpoint == null) {
                    errorMessages.add("http transform module[" + name + "].endpoint must not be null.");
                }

                return errorMessages;
            }

            public void setDefaults() {
                if(this.method == null) {
                    this.method = "get";
                }
                if(this.params == null) {
                    this.params = new HashMap<>();
                }
                if(this.headers == null) {
                    this.headers = new HashMap<>();
                }
            }

        }

        private static class ResponseParameters implements Serializable {

            private Format format;
            private SourceConfig.InputSchema schema;
            private List<Integer> acceptableStatusCodes;

            public List<String> validate(String name) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.format == null) {
                    errorMessages.add("http transform module[" + name + "].format must not be null.");
                }
                if(this.schema != null && (this.schema.getFields() == null || this.schema.getFields().isEmpty())) {
                    errorMessages.add("http transform module[" + name + "].schema.fields must not be empty.");
                }
                if(this.acceptableStatusCodes != null) {
                    for(final Integer acceptableStatusCode : acceptableStatusCodes) {
                        if(acceptableStatusCode == null) {
                            errorMessages.add("http transform module[" + name + "].acceptableStatusCodes value must not be null.");
                        } else if(acceptableStatusCode >= 600 || acceptableStatusCode < 100) {
                            errorMessages.add("http transform module[" + name + "].acceptableStatusCodes value[" + acceptableStatusCode + "] must be between 100 and 599");
                        }
                    }
                }
                return errorMessages;
            }

            public void setDefaults() {
                if(this.acceptableStatusCodes == null) {
                    this.acceptableStatusCodes = new ArrayList<>();
                }
            }

        }

        private static class RetryParameters implements Serializable {

            private BackoffParameters backoff;

            public List<String> validate(String name) {
                return new ArrayList<>();
            }

            public void setDefaults() {
                if(backoff == null) {
                    backoff = new BackoffParameters();
                }
                backoff.setDefaults();
            }

        }

        private static class BackoffParameters implements Serializable {

            private Double exponent;
            private Integer initialBackoffSecond;
            private Integer maxBackoffSecond;
            private Integer maxCumulativeBackoffSecond;
            private Integer maxRetries;

            public List<String> validate(String name) {
                return new ArrayList<>();
            }

            public void setDefaults() {
                // reference: FluentBackoff.DEFAULT
                if(exponent == null) {
                    exponent = 1.5;
                }
                if(initialBackoffSecond == null) {
                    initialBackoffSecond = 1;
                }
                if(maxBackoffSecond == null) {
                    maxBackoffSecond = 60 * 60 * 24 * 1000;
                }
                if(maxCumulativeBackoffSecond == null) {
                    maxCumulativeBackoffSecond = 60 * 60 * 24 * 1000;
                }
                if(maxRetries == null) {
                    this.maxRetries = Integer.MAX_VALUE;
                }
            }

        }

    }

    public enum Format {
        text,
        bytes,
        json
    }

    @Override
    public String getName() {
        return "http";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final List<Schema> inputSchemas = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag tag = new TupleTag<>() {};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.add(input.getSchema());
            tuple = tuple.and(tag, input.getCollection());
        }

        final HttpTransformParameters parameters = HttpTransformParameters.of(
                config.getParameters(),
                config.getName(),
                inputs.get(0).getCollection());


        final Schema responseSchema = createResponseSchema(parameters.response);
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.select, responseSchema.getFields(), parameters.outputType);
        final Schema outputSchema = createOutputSchema(responseSchema, selectFunctions);
        final Schema failureSchema = createFailureSchema();

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        switch (parameters.outputType) {
            case ROW -> {
                final TupleTag<Row> outputTag = new TupleTag<>() {};
                final TupleTag<Row> failureTag = new TupleTag<>() {};
                final Coder<Row> outputCoder = RowCoder.of(outputSchema);
                final Transform<Schema, Row> transform = new Transform<>(
                        config.getName(),
                        parameters,
                        selectFunctions,
                        s -> s,
                        JsonToRowConverter::convert,
                        RowSchemaUtil::create,
                        tags,
                        inputNames,
                        inputTypes,
                        parameters.outputType,
                        responseSchema,
                        outputSchema,
                        failureSchema,
                        outputCoder,
                        outputTag,
                        failureTag);

                final PCollectionTuple outputTuple = tuple.apply(config.getName(), transform);
                final PCollection<Row> output = outputTuple.get(outputTag);
                final PCollection<Row> failures = outputTuple.get(failureTag);
                final FCollection<?> outputFCollection = FCollection.of(config.getName(), output, DataType.ROW, outputSchema);
                final FCollection<?> failuresFCollection = FCollection.of(config.getName(), failures.setCoder(RowCoder.of(failureSchema)), DataType.ROW, failureSchema);
                outputs.put(config.getName(), outputFCollection);
                outputs.put(config.getName() + ".failures", failuresFCollection);
            }
            case AVRO -> {
                final TupleTag<GenericRecord> outputTag = new TupleTag<>() {};
                final TupleTag<GenericRecord> failureTag = new TupleTag<>() {};
                final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputSchema);
                final org.apache.avro.Schema failureAvroSchema = RowToRecordConverter.convertSchema(failureSchema);
                final Coder<GenericRecord> outputCoder = AvroGenericCoder.of(outputAvroSchema);
                final Transform<org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        config.getName(),
                        parameters,
                        selectFunctions,
                        RowToRecordConverter::convertSchema,
                        JsonToRecordConverter::convert,
                        AvroSchemaUtil::create,
                        tags,
                        inputNames,
                        inputTypes,
                        parameters.outputType,
                        responseSchema,
                        outputSchema,
                        failureSchema,
                        outputCoder,
                        outputTag,
                        failureTag);

                final PCollectionTuple outputTuple = tuple.apply(config.getName(), transform);
                final PCollection<GenericRecord> output = outputTuple.get(outputTag);
                final PCollection<GenericRecord> failures = outputTuple.get(failureTag);
                final FCollection<?> outputFCollection = FCollection.of(config.getName(), output, DataType.AVRO, outputAvroSchema);
                final FCollection<?> failuresFCollection = FCollection.of(config.getName(), failures.setCoder(AvroGenericCoder.of(failureAvroSchema)), DataType.AVRO, failureAvroSchema);
                outputs.put(config.getName(), outputFCollection);
                outputs.put(config.getName() + ".failures", failuresFCollection);
            }
            default -> throw new IllegalArgumentException("Not supported outputType: " + parameters.outputType);
        }

        return outputs;
    }

    private static Schema createResponseSchema(final HttpTransformParameters.ResponseParameters response) {
        final Schema.Field bodyField;
        if(response.schema == null) {
            bodyField = switch (response.format) {
                case bytes -> Schema.Field.of("body", Schema.FieldType.BYTES.withNullable(true));
                case text -> Schema.Field.of("body", Schema.FieldType.STRING.withNullable(true));
                case json -> Schema.Field.of("body", Schema.FieldType.STRING.withNullable(true))
                        .withOptions(Schema.Options.builder()
                                .setOption("sqlType", Schema.FieldType.STRING, "json"));
            };
        } else {
            bodyField = Schema.Field
                    .of("body", Schema.FieldType.row(SourceConfig.convertSchema(response.schema)));
        }
        return Schema.builder()
                .addField("statusCode", Schema.FieldType.INT32)
                .addField(bodyField)
                .addField("headers", Schema.FieldType.map(
                        Schema.FieldType.STRING,
                        Schema.FieldType.array(Schema.FieldType.STRING)).withNullable(true))
                .build();
    }

    private static Schema createOutputSchema(final Schema responseSchema, final List<SelectFunction> selectFunctions) {
        if(selectFunctions.isEmpty()) {
            return responseSchema;
        } else {
            return SelectFunction.createSchema(selectFunctions);
        }
    }

    private static Schema createFailureSchema() {
        return Schema.builder()
                .addField("message", Schema.FieldType.STRING.withNullable(true))
                .addField("request", Schema.FieldType.STRING.withNullable(true))
                .addField("stackTrace", Schema.FieldType.STRING.withNullable(true))
                .addField("timestamp", Schema.FieldType.DATETIME)
                .build();
    }

    public static class Transform<RuntimeSchemaT, T> extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final String name;
        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final DataType outputType;
        private final Coder<T> responseCoder;

        private final HttpTransformParameters.RequestParameters request;
        private final HttpTransformParameters.ResponseParameters response;
        private final HttpTransformParameters.RetryParameters retry;
        private final Integer timeoutSecond;
        private final List<SelectFunction> selectFunctions;

        private final SchemaUtil.SchemaConverter<Schema,RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.JsonConverter<RuntimeSchemaT,T> jsonConverter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

        private final Schema responseSchema;
        private final Schema outputSchema;
        private final Schema failureSchema;

        private final TupleTag<T> outputTag;
        private final TupleTag<T> failureTag;


        Transform(final String name,
                  final HttpTransformParameters parameters,
                  final List<SelectFunction> selectFunctions,
                  final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.JsonConverter<RuntimeSchemaT, T> jsonConverter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                  final List<TupleTag<?>> inputTags,
                  final List<String> inputNames,
                  final List<DataType> inputTypes,
                  final DataType outputType,
                  final Schema responseSchema,
                  final Schema outputSchema,
                  final Schema failureSchema,
                  final Coder<T> responseCoder,
                  final TupleTag<T> outputTag,
                  final TupleTag<T> faulureTag) {

            this.name = name;
            this.request = parameters.request;
            this.response = parameters.response;
            this.retry = parameters.retry;
            this.timeoutSecond = parameters.timeoutSecond;
            this.selectFunctions = selectFunctions;

            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.outputType = outputType;
            this.responseCoder = responseCoder;

            this.responseSchema = responseSchema;
            this.outputSchema = outputSchema;
            this.failureSchema = failureSchema;
            this.schemaConverter = schemaConverter;
            this.jsonConverter = jsonConverter;
            this.valueCreator = valueCreator;

            this.outputTag = outputTag;
            this.failureTag = faulureTag;

        }

        @Override
        public PCollectionTuple expand(PCollectionTuple inputs) {

            final PCollection<UnionValue> unionValues = inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames));

            final HttpCaller<RuntimeSchemaT,T> caller = new HttpCaller<>(
                    name, request, response, retry, timeoutSecond, selectFunctions,
                    schemaConverter, jsonConverter, valueCreator, responseSchema, outputSchema, outputType);
            RequestResponseIO<UnionValue, T> requestResponseIO = RequestResponseIO
                    .ofCallerAndSetupTeardown(caller, responseCoder)
                    .withTimeout(Duration.standardSeconds(timeoutSecond));

            final Result<T> httpResult = unionValues.apply("HttpCall", requestResponseIO);
            final PCollection<T> output = httpResult.getResponses();
            final PCollection<T> errors = httpResult.getFailures()
                    .apply("Failures", ParDo.of(new FailureDoFn<>(failureSchema, schemaConverter, valueCreator)));

            return PCollectionTuple
                    .of(outputTag, output)
                    .and(failureTag, errors);
        }

        private static class HttpCaller<RuntimeSchemaT,T> implements Caller<UnionValue, T>, SetupTeardown {

            private static final String METADATA_ENDPOINT = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/";

            private final String name;
            private final HttpTransformParameters.RequestParameters request;
            private final HttpTransformParameters.ResponseParameters response;
            private final HttpTransformParameters.RetryParameters retry;
            private final Integer timeoutSecond;

            private final SchemaUtil.SchemaConverter<Schema,RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.JsonConverter<RuntimeSchemaT,T> jsonConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

            private final Schema responseSchema;
            private final Schema outputSchema;
            private final DataType outputType;

            private final List<SelectFunction> selectFunctions;

            private final Set<TokenType> tokenTypes;

            private transient RuntimeSchemaT runtimeOutputSchema;
            private transient RuntimeSchemaT runtimeResponseSchema;

            private transient HttpClient client;
            private transient String idToken;
            private transient String accessToken;

            private transient Template templateEndpoint;
            private transient Map<String,Template> templateParams;
            private transient Map<String,Template> templateHeaders;
            private transient Template templateBody;

            public HttpCaller(
                    final String name,
                    final HttpTransformParameters.RequestParameters request,
                    final HttpTransformParameters.ResponseParameters response,
                    final HttpTransformParameters.RetryParameters retry,
                    final Integer timeoutSecond,
                    final List<SelectFunction> selectFunctions,
                    final SchemaUtil.SchemaConverter<Schema,RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.JsonConverter<RuntimeSchemaT,T> jsonConverter,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                    final Schema responseSchema,
                    final Schema outputSchema,
                    final DataType outputType) {

                this.name = name;
                this.request = request;
                this.response = response;
                this.retry = retry;
                this.timeoutSecond = timeoutSecond;
                this.selectFunctions = selectFunctions;

                this.schemaConverter = schemaConverter;
                this.jsonConverter = jsonConverter;
                this.valueCreator = valueCreator;

                this.responseSchema = responseSchema;
                this.outputSchema = outputSchema;
                this.outputType = outputType;

                this.tokenTypes = request.headers.entrySet().stream().map(
                        s -> {
                            if(!s.getKey().contains("Authorization")) {
                                return TokenType.none;
                            } else if(s.getValue().contains("_id_token")) {
                                return TokenType.id;
                            } else if(s.getValue().contains("_access_token")) {
                                return TokenType.access;
                            } else {
                                return TokenType.none;
                            }
                        })
                        .collect(Collectors.toSet());
            }

            @Override
            public void setup() throws UserCodeExecutionException {

                this.runtimeResponseSchema = schemaConverter.convert(responseSchema);
                this.runtimeOutputSchema = schemaConverter.convert(outputSchema);

                this.client = HttpClient.newBuilder().build();

                this.templateEndpoint = TemplateUtil.createStrictTemplate(name + "TemplateEndpoint", request.endpoint);
                this.templateParams = new HashMap<>();
                if(!this.request.params.isEmpty()) {
                    int count = 0;
                    for(final Map.Entry<String, String> entry : request.params.entrySet()) {
                        final Template template = TemplateUtil.createStrictTemplate("TemplateParams" + count, entry.getValue());
                        this.templateParams.put(entry.getKey(), template);
                        count++;
                    }
                }
                this.templateHeaders = new HashMap<>();
                if(!this.request.headers.isEmpty()) {
                    int count = 0;
                    for(final Map.Entry<String, String> entry : request.headers.entrySet()) {
                        final Template template = TemplateUtil.createStrictTemplate("TemplateHeaders" + count, entry.getValue());
                        this.templateHeaders.put(entry.getKey(), template);
                        count++;
                    }
                }
                if(this.request.body != null) {
                    this.templateBody = TemplateUtil.createStrictTemplate(name + "TemplateBody", request.body);
                }

                if(this.tokenTypes.contains(TokenType.id)) {
                    this.idToken = getIdToken(request.endpoint);
                }
                if(this.tokenTypes.contains(TokenType.access)) {
                    this.accessToken = getAccessToken();
                }

                for(final SelectFunction selectFunction : selectFunctions) {
                    selectFunction.setup();
                }
            }

            @Override
            public void teardown() {

            }

            @Override
            public T call(UnionValue unionValue) throws UserCodeExecutionException {
                try {
                    HttpResponse.BodyHandler<?> bodyHandler = switch (response.format) {
                        case text, json -> HttpResponse.BodyHandlers.ofString();
                        case bytes -> HttpResponse.BodyHandlers.ofByteArray();
                    };

                    HttpResponse<?> httpResponse = sendRequest(unionValue, bodyHandler);
                    if(httpResponse.statusCode() == 401
                            && (tokenTypes.contains(TokenType.id) || tokenTypes.contains(TokenType.access))) {

                        if(tokenTypes.contains(TokenType.id)) {
                            LOG.info("Try to reacquire id token");
                            this.idToken = getIdToken(request.endpoint);
                        }
                        if(tokenTypes.contains(TokenType.access)) {
                            LOG.info("Try to reacquire access token");
                            this.accessToken = getAccessToken();
                        }
                        httpResponse = sendRequest(unionValue, bodyHandler);
                    }

                    final boolean acceptable = response.acceptableStatusCodes.contains(httpResponse.statusCode());
                    if(httpResponse.statusCode() >= 400 && httpResponse.statusCode() < 500) {
                        if(!acceptable) {
                            final String errorMessage = "Illegal response code: " + httpResponse.statusCode() + ", for endpoint: " + request.endpoint + ", response: " + httpResponse.body();
                            LOG.error(errorMessage);
                            throw new UserCodeExecutionException(errorMessage);
                        } else {
                            LOG.info("Acceptable code: {}", httpResponse.statusCode());
                        }
                    }

                    final T output = switch (this.response.format) {
                        case text -> {
                            final String body = (String) httpResponse.body();
                            final Map<String, Object> values = new HashMap<>();
                            values.put("statusCode", httpResponse.statusCode());
                            values.put("body", body);
                            values.put("headers", httpResponse.headers().map());
                            values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
                            yield valueCreator.create(runtimeResponseSchema, values);
                        }
                        case bytes -> {
                            final byte[] body = (byte[]) httpResponse.body();
                            final Map<String, Object> values = new HashMap<>();
                            values.put("statusCode", httpResponse.statusCode());
                            values.put("body", body);
                            values.put("headers", httpResponse.headers().map());
                            values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
                            yield valueCreator.create(runtimeResponseSchema, values);
                        }
                        case json -> {
                            final String body = (String) httpResponse.body();
                            JsonElement responseJson = new Gson().fromJson(body, JsonElement.class);
                            if(!responseJson.isJsonObject()) {
                                responseJson = new JsonObject();
                            }

                            final JsonObject jsonObject = new JsonObject();
                            jsonObject.addProperty("statusCode", httpResponse.statusCode());
                            jsonObject.addProperty("timestamp", Instant.now().toString());
                            final JsonObject headers = new JsonObject();
                            for(final Map.Entry<String, List<String>> entry : httpResponse.headers().map().entrySet()) {
                                final JsonArray headerValues = new JsonArray();
                                entry.getValue().forEach(headerValues::add);
                                headers.add(entry.getKey(), headerValues);
                            }
                            jsonObject.add("headers", headers);
                            if(Schema.TypeName.ROW.equals(responseSchema.getField("body").getType().getTypeName())) {
                                jsonObject.add("body", responseJson.getAsJsonObject());
                            } else {
                                jsonObject.addProperty("body", responseJson.toString());
                            }
                            yield jsonConverter.convert(runtimeResponseSchema, jsonObject);
                        }
                    };
                    if(selectFunctions.isEmpty()) {
                        return output;
                    }
                    final Map<String, Object> primitives = SelectFunction.apply(selectFunctions, output, outputType, outputType, Instant.now());
                    return valueCreator.create(runtimeOutputSchema, primitives);
                } catch (URISyntaxException e) {
                    throw new UserCodeExecutionException(e);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (Throwable e) {
                    throw new RuntimeException("Failed to send request", e);
                }
            }

            private <ResponseT> HttpResponse<ResponseT> sendRequest(
                    final UnionValue unionValue,
                    final HttpResponse.BodyHandler<ResponseT> bodyHandler) throws Throwable {

                final Map<String, Object> values = unionValue.getMap();
                TemplateUtil.setFunctions(values);
                values.put("_id_token", idToken);
                values.put("_access_token", accessToken);
                final String url = createEndpoint(values);

                final String bodyText;
                if(this.request.body != null) {
                    bodyText = TemplateUtil.executeStrictTemplate(templateBody, values);
                } else {
                    bodyText = "";
                }
                final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(bodyText);

                HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .uri(new URI(url))
                        .timeout(java.time.Duration.ofSeconds(timeoutSecond))
                        .method(this.request.method, bodyPublisher);

                for(final Map.Entry<String, Template> entry : templateHeaders.entrySet()) {
                    final String headerValue = TemplateUtil.executeStrictTemplate(entry.getValue(), values);
                    builder = builder.header(entry.getKey(), headerValue);
                }

                return this.client.send(builder.build(), bodyHandler);
            }

            private String createEndpoint(final Map<String, Object> values) {
                final String params = templateParams.entrySet()
                        .stream()
                        .map(e -> e.getKey() + "=" + URLEncoder
                                .encode(TemplateUtil
                                        .executeStrictTemplate(e.getValue(), values), StandardCharsets.UTF_8))
                        .collect(Collectors.joining("&"));

                return TemplateUtil.executeStrictTemplate(templateEndpoint, values) + (params.isEmpty() ? "" : ("?" + params));
            }

            private String getIdToken(final String endpoint) throws UserCodeExecutionException {
                try {
                    final HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(new URI(METADATA_ENDPOINT + "identity?audience=" + endpoint))
                            .header("Metadata-Flavor", "Google")
                            .GET()
                            .build();
                    final HttpResponse<String> httpResponse = this.client
                            .send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    LOG.info("Succeed to get id token");
                    return httpResponse.body();
                } catch (URISyntaxException e) {
                    throw new UserCodeExecutionException("Failed to get id token", e);
                } catch (Throwable e) {
                    LOG.error("Failed to get id token cause: {}", e.getMessage());
                    throw new UserCodeRemoteSystemException("Failed to get id token", e);
                }
            }

            private String getAccessToken() throws UserCodeExecutionException {
                try {
                    final HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(new URI(METADATA_ENDPOINT + "token"))
                            .header("Metadata-Flavor", "Google")
                            .GET()
                            .build();
                    final HttpResponse<String> httpResponse = this.client
                            .send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    final JsonElement responseJson = new Gson().fromJson(httpResponse.body(), JsonElement.class);
                    if(!responseJson.isJsonObject()) {
                        throw new IllegalStateException("Illegal token response: " + responseJson);
                    }
                    final JsonObject jsonObject = responseJson.getAsJsonObject();
                    if(!jsonObject.has("access_token")) {
                        throw new IllegalStateException("Illegal token response: " + responseJson);
                    }

                    LOG.info("Succeed to get access token");
                    return jsonObject.get("access_token").getAsString();
                } catch (final URISyntaxException e) {
                    throw new UserCodeExecutionException("Failed to get access token", e);
                } catch (final Throwable e) {
                    LOG.error("Failed to get access token cause: {}", e.getMessage());
                    throw new UserCodeRemoteSystemException("Failed to get access token", e);
                }
            }

            public enum TokenType {
                id,
                access,
                none
            }
        }

        private static class FailureDoFn<RuntimeSchemaT,T> extends DoFn<ApiIOError, T> {

            private final Schema failureSchema;
            private final SchemaUtil.SchemaConverter<Schema,RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

            private transient RuntimeSchemaT runtimeSchema;

            FailureDoFn(
                    final Schema failureSchema,
                    final SchemaUtil.SchemaConverter<Schema,RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator) {

                this.failureSchema = failureSchema;
                this.schemaConverter = schemaConverter;
                this.valueCreator = valueCreator;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = schemaConverter.convert(failureSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final ApiIOError error = c.element();
                if(error == null) {
                    return;
                }
                final Map<String, Object> values = new HashMap<>();
                values.put("message", error.getMessage());
                values.put("request", error.getRequestAsString());
                values.put("stackTrace", error.getStackTrace());
                values.put("timestamp", error.getObservedTimestamp().getMillis() * 1000L);
                final T output = valueCreator.create(runtimeSchema, values);
                LOG.error("failure: {}", output);
                c.output(output);
            }

        }

    }
}