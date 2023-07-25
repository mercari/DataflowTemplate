package com.mercari.solution.module.transform;

import ai.onnxruntime.*;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import com.mercari.solution.util.gcp.StorageUtil;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ONNXTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(ONNXTransform.class);

    private class OnnxTransformParameters implements Serializable {

        private String model;
        private Long bufferSize;
        private Long bufferIntervalSeconds;
        private List<String> outputs;
        private List<String> groupFields;

        public String getModel() {
            return model;
        }

        public Long getBufferSize() {
            return bufferSize;
        }

        public Long getBufferIntervalSeconds() {
            return bufferIntervalSeconds;
        }

        public List<String> getOutputs() {
            return outputs;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            return errorMessages;
        }

        public void setDefaults() {
            if(this.bufferSize == null) {
                this.bufferSize = 100L;
            }
            if(this.bufferIntervalSeconds == null) {
                this.bufferIntervalSeconds = 10L;
            }
            if(this.outputs == null) {
                this.outputs = new ArrayList<>();
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
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
        parameters.validate();
        parameters.setDefaults();

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        final DataType outputType = OptionUtil.isStreaming(inputs.get(0).getCollection()) ? DataType.ROW : DataType.AVRO;

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final String outputFailureName = config.getName() + ".failures";
        final Schema outputFailureSchema = createFailureSchema();
        final KV<Map<String,NodeInfo>,Map<String,NodeInfo>> nodesInfo = ONNXRuntimeUtil.getNodesInfo(parameters.getModel());

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        switch (outputType) {
            case ROW: {
                final List<TupleTag<Row>> outputTags = new ArrayList<>();
                final List<Schema> outputSchemas = new ArrayList<>();
                for (final FCollection<?> input : inputs) {
                    final TupleTag<Row> outputTag = new TupleTag<>() {};
                    final Schema inputSchema = input.getSchema();
                    final Schema onnxOutputSchema = OnnxToRowConverter.convertOutputSchema(nodesInfo.getValue(), parameters.getOutputs());
                    final Schema outputSchema = RowSchemaUtil.merge(inputSchema, onnxOutputSchema);
                    outputTags.add(outputTag);
                    outputSchemas.add(outputSchema);
                }
                final TupleTag<Row> outputFailureTag = new TupleTag<>() {};

                final Transform<Schema, Schema, Row> transform = new Transform<>(
                        config.getName(),
                        parameters,
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
                for(int i=0; i<inputs.size(); i++) {
                    final String outputName = config.getName() + (inputs.size() > 1 ? "." + inputs.get(i).getName() : "");
                    final Schema outputSchema = outputSchemas.get(i);
                    final PCollection<Row> pCollection = output.get(outputTags.get(i));
                    final FCollection<?> fCollection = FCollection.of(outputName, pCollection.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
                    outputs.put(outputName, fCollection);
                }
                outputs.put(outputFailureName, FCollection.of(outputFailureName, output.get(outputFailureTag).setCoder(RowCoder.of(outputFailureSchema)), DataType.ROW, outputFailureSchema));
                break;
            }
            case AVRO: {
                final List<TupleTag<GenericRecord>> outputTags = new ArrayList<>();
                final List<String> outputSchemas = new ArrayList<>();
                for (final FCollection<?> input : inputs) {
                    final TupleTag<GenericRecord> outputTag = new TupleTag<>() {};
                    final org.apache.avro.Schema inputSchema = input.getAvroSchema();
                    final org.apache.avro.Schema onnxOutputSchema = OnnxToRecordConverter.convertOutputSchema(nodesInfo.getValue(), parameters.getOutputs());
                    final org.apache.avro.Schema outputSchema = AvroSchemaUtil.merge(inputSchema, onnxOutputSchema);
                    outputTags.add(outputTag);
                    outputSchemas.add(outputSchema.toString());
                }
                final TupleTag<GenericRecord> outputFailureTag = new TupleTag<>() {};

                final Transform<String, org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        config.getName(),
                        parameters,
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
                for(int i=0; i<inputs.size(); i++) {
                    final String outputName = config.getName() + (inputs.size() > 1 ? "." + inputs.get(i).getName() : "");
                    final org.apache.avro.Schema outputSchema = AvroSchemaUtil.convertSchema(outputSchemas.get(i));
                    final PCollection<GenericRecord> pCollection = output.get(outputTags.get(i));
                    final FCollection<GenericRecord> fCollection = FCollection.of(outputName, pCollection.setCoder(AvroCoder.of(outputSchema)), DataType.AVRO, outputSchema);
                    outputs.put(outputName, fCollection);
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Not supported outputType: " + outputType);
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
        private final String model;
        private Long bufferSize;
        private Long bufferIntervalSeconds;
        private final List<String> outputs;
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
                  final OnnxTransformParameters parameters,
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
            this.model = parameters.getModel();
            this.bufferSize = parameters.getBufferSize();
            this.bufferIntervalSeconds = parameters.getBufferIntervalSeconds();
            this.outputs = parameters.getOutputs();
            this.groupFields = parameters.getGroupFields();
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
                    .apply("Union", Union.withKey(inputTags, inputTypes, groupFields, inputNames))
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("Inference", ParDo.of(new InferenceDoFn<>(
                                    model, bufferSize, bufferIntervalSeconds,
                                    schemaConverter, onnxValueConverter,
                                    outputType, outputTags, outputSchemas, outputFailureTag, outputFailureSchema))
                            .withOutputTags(mainTupleTag, tupleTagList));
        }

        private static class InferenceDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<KV<String, UnionValue>, T> {

            private final String model;
            private final Long bufferSize;
            private final Long bufferIntervalSeconds;

            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final OnnxValueConverter onnxValueConverter;

            private final DataType outputType;
            private final List<TupleTag<T>> outputTags;
            private final List<InputSchemaT> outputInputSchemas;
            private final TupleTag<T> outputFailureTag;
            private final InputSchemaT outputFailureSchema;


            private transient List<RuntimeSchemaT> outputSchemas;
            private transient OrtEnvironment environment;
            private static OrtSession session;


            public InferenceDoFn(
                    final String model,
                    final Long bufferSize,
                    final Long bufferIntervalSeconds,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final OnnxValueConverter onnxValueConverter,
                    final DataType outputType,
                    final List<TupleTag<T>> outputTags,
                    final List<InputSchemaT> outputInputSchemas,
                    final TupleTag<T> outputFailureTag,
                    final InputSchemaT outputFailureSchema) {

                this.model = model;
                this.bufferSize = bufferSize;
                this.bufferIntervalSeconds = bufferIntervalSeconds;

                this.schemaConverter = schemaConverter;
                this.onnxValueConverter = onnxValueConverter;

                this.outputType = outputType;
                this.outputTags = outputTags;
                this.outputInputSchemas = outputInputSchemas;
                this.outputFailureTag = outputFailureTag;
                this.outputFailureSchema = outputFailureSchema;

            }

            @Setup
            public void setup() throws IOException, OrtException {

                this.outputSchemas = this.outputInputSchemas.stream()
                        .map(schemaConverter::convert)
                        .collect(Collectors.toList());

                environment = OrtEnvironment.getEnvironment();
                createSession();
            }

            @Teardown
            public void teardown() {
                closeSession();
            }

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final MultiOutputReceiver receiver) throws OrtException {

                inference(c.element().getValue(), receiver);
            }


            private void inference(
                    final UnionValue element,
                    final MultiOutputReceiver receiver)
                    throws OrtException {

                final List<UnionValue> unionValues = Lists.newArrayList(element);
                final Map<String, OnnxTensor> inputs = UnionValueToOnnxConverter.convert(environment, session.getInputInfo(), unionValues);
                try(final OrtSession.Result result = session.run(inputs)) {
                    final List<Map<String,Object>> updates = onnxValueConverter.convert(result);

                    for(int i=0; i<unionValues.size(); i++) {
                        final UnionValue unionValue = unionValues.get(i);
                        final TupleTag<T> outputTag = outputTags.get(unionValue.getIndex());
                        final T output = (T) (UnionValue.merge(unionValue, outputSchemas.get(unionValue.getIndex()), updates.get(i), outputType));
                        receiver.get(outputTag).output(output);
                    }
                }
            }

            synchronized private void createSession() throws OrtException {
                if(session == null) {
                    try (final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions()) {

                        sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);
                        try {
                            sessionOptions.registerCustomOpLibrary(ONNXRuntimeUtil.getExtensionsLibraryPath());
                        } catch (Throwable e) {
                            LOG.info("skip to load extensions library");
                        }

                        final byte[] bytes = StorageUtil.readBytes(model);
                        session = this.environment.createSession(bytes, sessionOptions);
                        LOG.info("setup onnx inference thread: " + Thread.currentThread().getId());
                        LOG.info("onnx model: " + ONNXRuntimeUtil.getModelDescription(session));
                    }
                } else {
                    LOG.info("skip setup session");
                }
            }

            synchronized private void closeSession() {
                if(session != null) {
                    try {
                        session.close();
                    } catch (OrtException e) {
                        LOG.error("failed to close session cause: " + e.getMessage());
                    }
                    session = null;
                    LOG.info("close session");
                }
            }

        }
    }

    private interface OnnxValueConverter extends Serializable {
        List<Map<String, Object>> convert(OrtSession.Result result);
    }

}
