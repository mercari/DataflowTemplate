package com.mercari.solution.module.transform;

import ai.onnxruntime.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ONNXTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(ONNXTransform.class);

    private static class OnnxTransformParameters implements Serializable {

        private ModelParameter model;
        private List<InferenceParameter> inferences;

        private Integer bufferSize;
        private Integer bufferIntervalSeconds;

        private List<String> groupFields;

        public ModelParameter getModel() {
            return model;
        }

        public List<InferenceParameter> getInferences() {
            return inferences;
        }

        public Integer getBufferSize() {
            return bufferSize;
        }

        public Integer getBufferIntervalSeconds() {
            return bufferIntervalSeconds;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public void validate(final String name, final Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(model == null) {
                errorMessages.add("onnx transform module[" + name + "].model must not be null");
            } else {
                errorMessages.addAll(model.validate(name, inputSchemas));
            }
            if(inferences != null) {
                for(int index=0; index<inferences.size(); index++) {
                    errorMessages.addAll(inferences.get(index).validate(name, index, inputSchemas));
                }
            }

            if(bufferSize != null && bufferSize < 1) {
                errorMessages.add("onnx transform module[" + name + "].bufferSize parameter must be over than 0.");
            }
            if(bufferIntervalSeconds != null && bufferIntervalSeconds < 1) {
                errorMessages.add("onnx transform module[" + name + "].bufferIntervalSeconds parameter must be over than 0.");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            model.setDefaults();
            if(inferences == null) {
                inferences = new ArrayList<>();
            } else {
                for(InferenceParameter inference : inferences) {
                    inference.setDefaults();
                }
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }

            if(bufferSize == null) {
                bufferSize = 1;
            }
            if(bufferIntervalSeconds == null) {
                bufferIntervalSeconds = 1;
            }

        }

    }

    private static class ModelParameter implements Serializable {

        private String path;
        private OrtSession.SessionOptions.OptLevel optLevel;
        private OrtSession.SessionOptions.ExecutionMode executionMode;
        private List<SourceConfig.InputSchemaField> outputSchemaFields;

        public String getPath() {
            return path;
        }

        public OrtSession.SessionOptions.OptLevel getOptLevel() {
            return optLevel;
        }

        public OrtSession.SessionOptions.ExecutionMode getExecutionMode() {
            return executionMode;
        }

        public Schema getModelOutputSchema() {
            if (outputSchemaFields != null && outputSchemaFields.size() > 0) {
                return SourceConfig.convertSchema(outputSchemaFields);
            } else {
                final KV<Map<String,NodeInfo>,Map<String,NodeInfo>> nodesInfo = ONNXRuntimeUtil.getNodesInfo(path);
                return OnnxToRowConverter.convertOutputSchema(nodesInfo.getValue(), null);
            }
        }

        public List<String> validate(String name, Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.path == null) {
                errorMessages.add("onnx transform module[" + name + "].model.path parameter must not be null.");
            } else if(!path.startsWith("gs://")) {
                errorMessages.add("onnx transform module[" + name + "].model.path must start with gs://");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(optLevel == null) {
                optLevel = OrtSession.SessionOptions.OptLevel.BASIC_OPT;
            }
            if(executionMode == null) {
                executionMode = OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL;
            }
        }

    }

    private static class InferenceParameter implements Serializable {

        private String input;
        private List<MappingParameter> mappings;
        private JsonArray select;

        public String getInput() {
            return input;
        }

        public List<MappingParameter> getMappings() {
            return mappings;
        }

        public JsonArray getSelect() {
            return select;
        }

        public Schema mergeSchema(final Schema inputSchema, final Schema onnxOutputSchema) {
            final Schema schema = mergeOnnxSchema(inputSchema, onnxOutputSchema);
            if(select == null || select.isJsonNull() || select.isEmpty()) {
                return schema;
            }
            return SelectFunction.createSchema(select, schema.getFields());
        }

        public Schema mergeOnnxSchema(final Schema inputSchema, final Schema onnxOutputSchema) {
            final List<Schema.Field> outputFields = new ArrayList<>();
            if(mappings.size() > 0) {
                for(final MappingParameter mapping : mappings) {
                    for(final Map.Entry<String, String> output : mapping.getOutputs().entrySet()) {
                        final String onnxOutputFieldName = output.getKey();
                        final String outputFieldName = output.getValue();
                        if(!onnxOutputSchema.hasField(onnxOutputFieldName)) {
                            throw new IllegalStateException("outputs.key: " + onnxOutputFieldName + " is missing in onnx output schema: " + onnxOutputSchema);
                        }
                        final Schema.Field field = onnxOutputSchema.getField(onnxOutputFieldName);
                        outputFields.add(Schema.Field.of(outputFieldName, field.getType())
                                .withNullable(field.getType().getNullable())
                                .withOptions(field.getOptions()));
                    }
                }
            } else {
                outputFields.addAll(onnxOutputSchema.getFields());
            }
            return RowSchemaUtil.toBuilder(inputSchema)
                    .addFields(outputFields)
                    .build();
        }

        public InferenceSetting toSetting(final List<Schema.Field> inputFields, final DataType outputType) {
            final InferenceSetting inferenceSetting = new InferenceSetting();
            inferenceSetting.input = this.input;
            inferenceSetting.mappings = this.mappings;

            if(this.select != null) {
                inferenceSetting.selectFunctions = SelectFunction.of(this.select, inputFields, outputType);
            } else {
                inferenceSetting.selectFunctions = new ArrayList<>();
            }

            return inferenceSetting;
        }

        public List<String> validate(String name, int index, Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("onnx transform module[" + name + "].inferences[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(mappings == null) {
                mappings = new ArrayList<>();
            }
        }

        public static InferenceParameter defaultInference(final String input) {
            final InferenceParameter inferenceParameter = new InferenceParameter();
            inferenceParameter.input = input;
            inferenceParameter.setDefaults();
            return inferenceParameter;
        }

    }

    private static class MappingParameter implements Serializable {

        private Map<String, String> inputs;
        private Map<String, String> outputs;

        public Map<String, String> getInputs() {
            return inputs;
        }

        public Map<String, String> getOutputs() {
            return outputs;
        }

        public List<String> validate(String name, int index, Map<String, KV<DataType, Schema>> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.inputs == null) {
                errorMessages.add("onnx transform module[" + name + "].inferences[" + index + "].inputs parameter must not be null.");
            }
            if(this.outputs == null) {
                errorMessages.add("onnx transform module[" + name + "].inferences[" + index + "].outputs parameter must not be null.");
            }
            return errorMessages;
        }

    }

    private static class InferenceSetting implements Serializable {

        private String input;
        private List<MappingParameter> mappings;
        private List<SelectFunction> selectFunctions;

        public String getInput() {
            return input;
        }

        public List<MappingParameter> getMappings() {
            return mappings;
        }

        public void setup() {
            if(selectFunctions != null) {
                for(final SelectFunction selectFunction : selectFunctions) {
                    selectFunction.setup();
                }
            }
        }

    }

    @Override
    public String getName() {
        return "onnx";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final OnnxTransformParameters parameters = new Gson().fromJson(config.getParameters(), OnnxTransformParameters.class);
        if (parameters == null) {
            throw new IllegalArgumentException("OnnxTransform config parameters must not be empty!");
        }

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final Map<String, Schema> inputSchemas = new HashMap<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.put(input.getName(), input.getSchema());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        parameters.validate(config.getName(), inputSchemas);
        parameters.setDefaults();

        final String outputFailureName = config.getName() + ".failures";
        final Schema outputFailureSchema = createFailureSchema();
        final Schema outputOnnxSchema = parameters.getModel().getModelOutputSchema();
        final DataType outputType = OptionUtil.isStreaming(inputs.get(0).getCollection()) ? DataType.ROW : DataType.AVRO;

        final List<InferenceParameter> inferences = new ArrayList<>();
        for(final String inputName : inputNames) {
            final InferenceParameter inference = parameters.getInferences().stream()
                    .filter(i -> i.getInput().equals(inputName))
                    .findFirst()
                    .orElse(InferenceParameter.defaultInference(inputName));
            inferences.add(inference);
        }

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        switch (outputType) {
            case ROW -> {
                final List<TupleTag<Row>> outputTags = new ArrayList<>();
                final List<Schema> outputSchemas = new ArrayList<>();
                final List<InferenceSetting> inferenceSettings = new ArrayList<>();
                for (int index=0; index<inputs.size(); index++){
                    final FCollection<?> input = inputs.get(index);
                    final TupleTag<Row> outputTag = new TupleTag<>() {};
                    final Schema inputSchema = input.getSchema();
                    final InferenceParameter inference = inferences.get(index);
                    final Schema outputMediumSchema = inference.mergeOnnxSchema(inputSchema, outputOnnxSchema);
                    final Schema outputSchema = inference.mergeSchema(inputSchema, outputOnnxSchema);

                    outputTags.add(outputTag);
                    outputSchemas.add(outputSchema);
                    inferenceSettings.add(inference.toSetting(outputMediumSchema.getFields(), outputType));
                }
                final TupleTag<Row> outputFailureTag = new TupleTag<>() {};

                final Transform<Schema, Schema, Row> transform = new Transform<>(
                        config.getName(),
                        parameters.getModel(),
                        inferenceSettings,
                        parameters.getBufferSize(),
                        s -> s,
                        OnnxToRowConverter::convert,
                        inputTags,
                        inputNames,
                        inputTypes,
                        outputType,
                        outputTags,
                        outputSchemas,
                        outputFailureTag,
                        outputFailureSchema);

                final PCollectionTuple output = tuple.apply(config.getName(), transform);
                for (int i = 0; i < inputs.size(); i++) {
                    final String outputName = config.getName() + (inputs.size() > 1 ? "." + inputs.get(i).getName() : "");
                    final Schema outputSchema = outputSchemas.get(i);
                    final PCollection<Row> pCollection = output.get(outputTags.get(i));
                    final FCollection<?> fCollection = FCollection.of(outputName, pCollection.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
                    outputs.put(outputName, fCollection);
                }
                outputs.put(outputFailureName, FCollection.of(outputFailureName, output.get(outputFailureTag).setCoder(RowCoder.of(outputFailureSchema)), DataType.ROW, outputFailureSchema));
            }
            case AVRO -> {
                final List<TupleTag<GenericRecord>> outputTags = new ArrayList<>();
                final List<String> outputSchemas = new ArrayList<>();
                final List<InferenceSetting> inferenceSettings = new ArrayList<>();
                for (int index=0; index<inputs.size(); index++){
                    final FCollection<?> input = inputs.get(index);
                    final TupleTag<GenericRecord> outputTag = new TupleTag<>() {};
                    final Schema inputSchema = input.getSchema();
                    final InferenceParameter inference = inferences.get(index);
                    final Schema outputMediumSchema = inference.mergeOnnxSchema(inputSchema, outputOnnxSchema);
                    final Schema outputSchema = inference.mergeSchema(inputSchema, outputOnnxSchema);
                    final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputSchema);

                    outputTags.add(outputTag);
                    outputSchemas.add(outputAvroSchema.toString());
                    inferenceSettings.add(inference.toSetting(outputMediumSchema.getFields(), outputType));
                }
                final TupleTag<GenericRecord> outputFailureTag = new TupleTag<>() {};

                final Transform<String, org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        config.getName(),
                        parameters.getModel(),
                        inferenceSettings,
                        parameters.getBufferSize(),
                        AvroSchemaUtil::convertSchema,
                        OnnxToRecordConverter::convert,
                        inputTags,
                        inputNames,
                        inputTypes,
                        outputType,
                        outputTags,
                        outputSchemas,
                        outputFailureTag,
                        RowToRecordConverter.convertSchema(outputFailureSchema).toString());

                final PCollectionTuple output = tuple.apply(config.getName(), transform);
                for (int i = 0; i < inputs.size(); i++) {
                    final String outputName = config.getName() + (inputs.size() > 1 ? "." + inputs.get(i).getName() : "");
                    final org.apache.avro.Schema outputSchema = AvroSchemaUtil.convertSchema(outputSchemas.get(i));
                    final PCollection<GenericRecord> pCollection = output.get(outputTags.get(i));
                    final FCollection<GenericRecord> fCollection = FCollection.of(outputName, pCollection.setCoder(AvroCoder.of(outputSchema)), DataType.AVRO, outputSchema);
                    outputs.put(outputName, fCollection);
                }
            }
            default -> throw new IllegalArgumentException("Not supported outputType: " + outputType);
        }

        return outputs;
    }

    private static Schema createFailureSchema() {
        return Schema.builder()
                .addField("timestamp", Schema.FieldType.DATETIME)
                .addField("source", Schema.FieldType.STRING)
                .build();
    }


    public static class Transform<InputSchemaT, RuntimeSchemaT, T> extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final String name;
        private final ModelParameter model;
        private final List<InferenceSetting> inferences;
        private final Integer bufferSize;
        private final List<String> groupFields;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final OnnxValueConverter onnxValueConverter;

        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        private final DataType outputType;
        private final List<TupleTag<T>> outputTags;
        private final List<InputSchemaT> outputSchemas;
        private final TupleTag<T> outputFailureTag;
        private final InputSchemaT outputFailureSchema;

        Transform(final String name,
                  final ModelParameter model,
                  final List<InferenceSetting> inferences,
                  final Integer bufferSize,
                  final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                  final OnnxValueConverter onnxValueConverter,
                  final List<TupleTag<?>> inputTags,
                  final List<String> inputNames,
                  final List<DataType> inputTypes,
                  final DataType outputType,
                  final List<TupleTag<T>> outputTags,
                  final List<InputSchemaT> outputSchemas,
                  final TupleTag<T> outputFailureTag,
                  final InputSchemaT outputFailureSchema) {

            this.name = name;
            this.model = model;
            this.inferences = inferences;
            this.bufferSize = bufferSize;

            this.groupFields = new ArrayList<>();
            this.schemaConverter = schemaConverter;
            this.onnxValueConverter = onnxValueConverter;

            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;

            this.outputType = outputType;
            this.outputTags = outputTags;
            this.outputSchemas = outputSchemas;

            this.outputFailureTag = outputFailureTag;
            this.outputFailureSchema = outputFailureSchema;
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple inputs) {

            final TupleTag<T> mainTupleTag = outputTags.size() == 1 ? outputTags.get(0) : outputFailureTag;
            final TupleTagList tupleTagList = TupleTagList.of(outputTags.stream()
                    .filter(t -> !t.getId().equals(mainTupleTag.getId()))
                    .collect(Collectors.toList()));

            return inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames))
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("Inference", ParDo.of(new InferenceDoFn<>(
                                    name, model, inferences, bufferSize,
                                    schemaConverter, onnxValueConverter,
                                    outputType, outputTags, outputSchemas, outputFailureTag, outputFailureSchema))
                            .withOutputTags(mainTupleTag, tupleTagList));
        }

        private static class InferenceDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<UnionValue, T> {

            private final String name;
            private final ModelParameter model;
            private final List<InferenceSetting> inferences;
            private final Integer bufferSize;

            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final OnnxValueConverter onnxValueConverter;

            private final DataType outputType;
            private final List<TupleTag<T>> outputTags;
            private final List<InputSchemaT> outputInputSchemas;
            private final TupleTag<T> outputFailureTag;
            private final InputSchemaT outputFailureSchema;

            private final int maxLevel;
            private final List<List<MappingParameter>> mappingsList;


            private transient List<RuntimeSchemaT> outputSchemas;
            private transient OrtEnvironment environment;
            private static final Map<String, OrtSession> sessions = new HashMap<>();// Collections.synchronizedMap(new HashMap<>());

            private transient List<UnionValue> buffer;


            public InferenceDoFn(
                    final String name,
                    final ModelParameter model,
                    final List<InferenceSetting> inferences,
                    final Integer bufferSize,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final OnnxValueConverter onnxValueConverter,
                    final DataType outputType,
                    final List<TupleTag<T>> outputTags,
                    final List<InputSchemaT> outputInputSchemas,
                    final TupleTag<T> outputFailureTag,
                    final InputSchemaT outputFailureSchema) {

                this.name = name;
                this.model = model;
                this.inferences = inferences;
                this.bufferSize = bufferSize;

                this.schemaConverter = schemaConverter;
                this.onnxValueConverter = onnxValueConverter;

                this.outputType = outputType;
                this.outputTags = outputTags;
                this.outputInputSchemas = outputInputSchemas;
                this.outputFailureTag = outputFailureTag;
                this.outputFailureSchema = outputFailureSchema;

                this.maxLevel = inferences.stream()
                        .map(i -> i.getMappings().size())
                        .max(Integer::compareTo)
                        .orElse(1);

                this.mappingsList = new ArrayList<>();
                for(int level=0; level<maxLevel; level++) {
                    final List<MappingParameter> mappings = new ArrayList<>();
                    for (int inputIndex = 0; inputIndex < inferences.size(); inputIndex++) {
                        final InferenceSetting inference = inferences.get(inputIndex);
                        final MappingParameter mapping;
                        if (inference.getMappings().size() > level) {
                            mapping = inference.getMappings().get(level);
                        } else {
                            mapping = null;
                        }
                        mappings.add(mapping);
                    }
                    this.mappingsList.add(mappings);
                }
            }

            @Setup
            public void setup() throws OrtException {
                LOG.info("setup onnx module: " + name + ", inference thread: " + Thread.currentThread().getId());
                this.outputSchemas = this.outputInputSchemas.stream()
                        .map(schemaConverter::convert)
                        .collect(Collectors.toList());

                this.environment = OrtEnvironment.getEnvironment();
                createSession(name, model, environment);

                this.buffer = new ArrayList<>();

                for(final InferenceSetting inference : inferences) {
                    inference.setup();
                }
            }

            @Teardown
            public void teardown() {
                LOG.info("teardown onnx module: " + name + ", inference thread: " + Thread.currentThread().getId());
                closeSession(name);
            }

            /*
            @StartBundle
            public void startBundle(final StartBundleContext c) {
                this.buffer.clear();
            }

            @FinishBundle
            public void finishBundle(final FinishBundleContext c) throws OrtException {
                if(buffer.size() > 0) {
                    inference(buffer, receiver);
                    buffer.clear();
                }
            }
             */

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final MultiOutputReceiver receiver) throws OrtException {

                final UnionValue element = c.element();
                if(element == null) {
                    return;
                }

                /*
                buffer.add(element);
                if(buffer.size() >= bufferSize) {
                    inference(buffer, receiver);
                    buffer.clear();
                }
                 */

                inference(List.of(element), receiver);

            }

            private void inference(
                    final List<UnionValue> unionValues,
                    final MultiOutputReceiver receiver)
                    throws OrtException {

                final List<Map<String,Object>> updatesList = unionValues.stream()
                        .map(UnionValue::asPrimitiveMap)
                        .toList();
                for(int level=0; level<maxLevel; level++) {
                    final List<MappingParameter> mappings = mappingsList.get(level);

                    final Map<Integer,Integer> unionValueIndexMap = new HashMap<>();
                    final List<UnionValue> targetUnionValues = new ArrayList<>();
                    final List<Map<String,String>> renameFieldsList = new ArrayList<>();
                    final Set<String> requestedOutputs = new HashSet<>();
                    for(int unionValueIndex=0; unionValueIndex<unionValues.size(); unionValueIndex++) {
                        final UnionValue unionValue = unionValues.get(unionValueIndex);
                        final MappingParameter mapping = mappings.get(unionValue.getIndex());
                        if(mapping != null) {
                            final int targetUnionValueIndex = targetUnionValues.size();
                            unionValueIndexMap.put(targetUnionValueIndex, unionValueIndex);
                            targetUnionValues.add(unionValue);
                            renameFieldsList.add(mapping.getInputs());
                            requestedOutputs.addAll(mapping.getOutputs().keySet());
                        }
                    }

                    final OrtSession session = getOrCreateSession(name, model, environment);
                    final Map<String, OnnxTensor> inputs = UnionValueToOnnxConverter.convert(environment, session.getInputInfo(), targetUnionValues, renameFieldsList);
                    try(final OrtSession.Result result = session.run(inputs, requestedOutputs)) {
                        final List<Map<String,Object>> updates = onnxValueConverter.convert(result);
                        for(int targetUnionValueIndex=0; targetUnionValueIndex<targetUnionValues.size(); targetUnionValueIndex++) {
                            final Map<String, Object> values = updates.get(targetUnionValueIndex);
                            final Integer unionValueIndex = unionValueIndexMap.get(targetUnionValueIndex);
                            final int inputIndex = unionValues.get(unionValueIndex).getIndex();
                            final MappingParameter mapping = mappings.get(inputIndex);
                            if(mapping == null || mapping.getOutputs() == null) {
                                updatesList.get(unionValueIndex).putAll(values);
                            } else {
                                final Map<String, Object> renamedValues = new HashMap<>();
                                for(Map.Entry<String, Object> entry : values.entrySet()) {
                                    final String field = mapping.getOutputs().getOrDefault(entry.getKey(), entry.getKey());
                                    renamedValues.put(field, entry.getValue());
                                }
                                updatesList.get(unionValueIndex).putAll(renamedValues);
                            }
                        }
                    }
                }

                for(int unionValueIndex=0; unionValueIndex<unionValues.size(); unionValueIndex++) {
                    final UnionValue unionValue = unionValues.get(unionValueIndex);
                    final TupleTag<T> outputTag = outputTags.get(unionValue.getIndex());
                    final Map<String, Object> values = updatesList.get(unionValueIndex);
                    final InferenceSetting inference = inferences.get(unionValue.getIndex());
                    final RuntimeSchemaT outputSchema = outputSchemas.get(unionValue.getIndex());
                    final T output;
                    if(inference.selectFunctions != null && inference.selectFunctions.size() > 0) {
                        final Map<String, Object> selectedValues = SelectFunction.apply(inference.selectFunctions, values, outputType);
                        output = (T) (UnionValue.merge(unionValue, outputSchema, selectedValues, outputType));
                    } else {
                        output = (T) (UnionValue.merge(unionValue, outputSchema, values, outputType));
                    }

                    receiver.get(outputTag).output(output);
                }
            }

            synchronized static private OrtSession getOrCreateSession(final String name, final ModelParameter model, final OrtEnvironment environment) throws OrtException {
                if(sessions.containsKey(name)) {
                    final OrtSession session = sessions.get(name);
                    if(session != null) {
                        return session;
                    } else {
                        sessions.remove(name);
                    }
                }
                createSession(name, model, environment);
                return sessions.get(name);
            }

            synchronized static private void createSession(
                    final String name,
                    final ModelParameter model,
                    final OrtEnvironment environment) throws OrtException {

                if(sessions.containsKey(name) && sessions.get(name) == null) {
                    sessions.remove(name);
                }

                if(!sessions.containsKey(name)) {
                    try (final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions()) {

                        sessionOptions.setOptimizationLevel(model.getOptLevel());
                        sessionOptions.setExecutionMode(model.getExecutionMode());
                        try {
                            sessionOptions.registerCustomOpLibrary(ONNXRuntimeUtil.getExtensionsLibraryPath());
                        } catch (Throwable e) {
                            LOG.warn("setup onnx module: " + name + " is creating session. skipped to load extensions library");
                        }

                        final byte[] bytes = StorageUtil.readBytes(model.getPath());
                        final OrtSession session = environment.createSession(bytes, sessionOptions);
                        sessions.put(name, session);
                        LOG.info("setup onnx module: " + name + " created session");
                        LOG.info("onnx model: " + ONNXRuntimeUtil.getModelDescription(session));
                    }
                } else {
                    LOG.info("setup onnx module: " + name + " skipped creating session");
                }
            }

            synchronized static private void closeSession(final String name) {
                if(sessions.containsKey(name)) {
                    try(final OrtSession session = sessions.remove(name);) {
                        LOG.info("teardown onnx module: " + name + " closed session");
                    } catch (final OrtException e) {
                        LOG.error("teardown onnx module: " + name + " failed to close session cause: " + e.getMessage());
                    }
                }
            }

        }
    }

    private interface OnnxValueConverter extends Serializable {
        List<Map<String, Object>> convert(OrtSession.Result result);
    }

}
