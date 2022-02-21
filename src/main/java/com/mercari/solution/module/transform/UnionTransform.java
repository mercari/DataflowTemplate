package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.RowToMutationConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class UnionTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(UnionTransform.class);

    private class UnionTransformParameters {

        private String baseInput;
        private List<MappingParameter> mappings;
        private DataType outputType;

        public String getBaseInput() {
            return baseInput;
        }

        public void setBaseInput(String baseInput) {
            this.baseInput = baseInput;
        }

        public List<MappingParameter> getMappings() {
            return mappings;
        }

        public void setMappings(List<MappingParameter> mappings) {
            this.mappings = mappings;
        }

        public DataType getOutputType() {
            return outputType;
        }

        public void setOutputType(DataType outputType) {
            this.outputType = outputType;
        }
    }

    private class MappingParameter implements Serializable {

        private String outputField;
        private List<MappingInputParameter> inputs;

        public String getOutputField() {
            return outputField;
        }

        public void setOutputField(String outputField) {
            this.outputField = outputField;
        }

        public List<MappingInputParameter> getInputs() {
            return inputs;
        }

        public void setInputs(List<MappingInputParameter> inputs) {
            this.inputs = inputs;
        }
    }

    private class MappingInputParameter implements Serializable {

        private String input;
        private String field;

        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }
    }

    public String getName() { return "union"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return Collections.singletonMap(config.getName(), UnionTransform.transform(inputs, config));
    }

    private static void validateParameters(final UnionTransformParameters parameters, final List<String> inputs) {
        if(parameters == null) {
            throw new IllegalArgumentException("UnionTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getMappings() != null) {
            int index = 0;
            for(final MappingParameter mapping : parameters.getMappings()) {
                if(mapping.getOutputField() == null) {
                    errorMessages.add("UnionTransform parameter mappings[" + index + "].outputField parameters must not be null.");
                }
                if(mapping.getInputs() == null) {
                    errorMessages.add("UnionTransform parameter mappings[" + index + "].inputs parameters must not be null.");
                } else {
                    int indexInput = 0;
                    for(final MappingInputParameter mappingInput : mapping.getInputs()) {
                        if(mappingInput.getInput() == null) {
                            errorMessages.add("UnionTransform parameter mappings[" + index + "].inputs[" + indexInput +"].input parameters must not be null.");
                        } else if(!inputs.contains(mappingInput.getInput())){
                            errorMessages.add("UnionTransform parameter mappings[" + index + "].inputs[" + indexInput +"].input does not exists in inputs: " + inputs);
                        }
                        if(mappingInput.getField() == null) {
                            errorMessages.add("UnionTransform parameter mappings[" + index + "].inputs[" + indexInput +"].field parameters must not be null.");
                        }
                        indexInput++;
                    }
                }
                index++;
            }
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(UnionTransformParameters parameters) {
        if(parameters.getMappings() == null) {
            parameters.setMappings(new ArrayList<>());
        }
    }

    public static FCollection<?> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final UnionTransformParameters parameters = new Gson()
                .fromJson(config.getParameters(), UnionTransformParameters.class);
        final DataType outputType = selectDataType(parameters, inputs);

        validateParameters(parameters, inputs.stream().map(FCollection::getName).collect(Collectors.toList()));
        setDefaultParameters(parameters);

        final Schema schema = createUnionSchema(inputs, parameters);
        final Map<String, Map<String, String>> renameMap = createRenameMap(parameters);

        PCollectionTuple unionInputs = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            unionInputs = unionInputs.and(input.getName(), input.getCollection());
        }

        final Map<String, FCollection<?>> collections = inputs.stream()
                .collect(Collectors.toMap(FCollection::getName, i -> i));

        final PCollection<?> output = unionInputs
                .apply("Union" + config.getName(), new Union(outputType, schema, collections, renameMap));
        return FCollection.of(config.getName(), output, outputType, schema);
    }

    public static class Union extends PTransform<PCollectionTuple, PCollection<?>> {

        private final DataType outputType;
        private final Schema outputSchema;
        private final Map<String, FCollection<?>> collections;
        private final Map<String, Map<String, String>> renameMap;

        public Union(final DataType outputType,
                     final Schema outputSchema,
                     final Map<String, FCollection<?>> collections,
                     final Map<String, Map<String, String>> renameMap) {
            this.outputType = outputType;
            this.outputSchema = outputSchema;
            this.collections = collections;
            this.renameMap = renameMap;
        }

        @Override
        public PCollection<?> expand(final PCollectionTuple tuple) {
            switch (outputType) {
                case AVRO: {
                    final org.apache.avro.Schema avroSchema = RowToRecordConverter.convertSchema(outputSchema);
                    PCollectionList<GenericRecord> formattedList = PCollectionList.empty(tuple.getPipeline());
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> input : tuple.getAll().entrySet()) {
                        final PCollection<GenericRecord> records = input.getValue()
                                .apply("Format" + input.getKey().getId(), new Format<>(
                                        outputType,
                                        collections.get(input.getKey().getId()),
                                        avroSchema.toString(),
                                        AvroSchemaUtil::convertSchema,
                                        (org.apache.avro.Schema s, GenericRecord r, Map<String, String> rf) -> AvroSchemaUtil.toBuilder(s, r, rf).build(),
                                        renameMap.getOrDefault(input.getKey().getId(), new HashMap<>())))
                                .setCoder(AvroCoder.of(avroSchema));
                        formattedList = formattedList.and(records);
                    }
                    return formattedList.apply("Flatten", Flatten.pCollections());
                }
                case ROW: {
                    PCollectionList<Row> formattedList = PCollectionList.empty(tuple.getPipeline());
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> input : tuple.getAll().entrySet()) {
                        final PCollection<Row> rows = input.getValue()
                                .apply("Format" + input.getKey().getId(), new Format<>(
                                        outputType,
                                        collections.get(input.getKey().getId()),
                                        outputSchema,
                                        s -> s,
                                        (Schema s, Row r, Map<String, String> rf) -> RowSchemaUtil.toBuilder(s, r, rf).build(),
                                        renameMap.getOrDefault(input.getKey().getId(), new HashMap<>())))
                                .setCoder(RowCoder.of(outputSchema));
                        formattedList = formattedList.and(rows);
                    }
                    return formattedList.apply("Flatten", Flatten.pCollections());
                }
                case STRUCT: {
                    final Type type = RowToMutationConverter.convertSchema(outputSchema);
                    PCollectionList<Struct> formattedList = PCollectionList.empty(tuple.getPipeline());
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> input : tuple.getAll().entrySet()) {
                        final PCollection<Struct> structs = input.getValue()
                                .apply("Format" + input.getKey().getId(), new Format<>(
                                        outputType,
                                        collections.get(input.getKey().getId()),
                                        type,
                                        s -> s,
                                        (Type t, Struct s, Map<String, String> rf) -> StructSchemaUtil.toBuilder(t, s, rf).build(),
                                        renameMap.getOrDefault(input.getKey().getId(), new HashMap<>())));
                        formattedList = formattedList.and(structs);
                    }
                    return formattedList.apply("Flatten", Flatten.pCollections());
                }
                case ENTITY: {
                    PCollectionList<Entity> formattedList = PCollectionList.empty(tuple.getPipeline());
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> input : tuple.getAll().entrySet()) {
                        final PCollection<Entity> entities = input.getValue()
                                .apply("Format" + input.getKey().getId(), new Format<>(
                                        outputType,
                                        collections.get(input.getKey().getId()),
                                        outputSchema,
                                        s -> s,
                                        (Schema s, Entity e, Map<String, String> rf) -> EntitySchemaUtil.toBuilder(s, e, rf).build(),
                                        renameMap.getOrDefault(input.getKey().getId(), new HashMap<>())));
                        formattedList = formattedList.and(entities);
                    }
                    return formattedList.apply("Flatten", Flatten.pCollections());
                }
                default:
                    throw new IllegalArgumentException("Union module not support output data typ: " + outputType.name());
            }

        }

    }

    public static class Format<T,InputSchemaT,RuntimeSchemaT> extends PTransform<PCollection<?>, PCollection<T>> {

        private final DataType outputType;
        private final FCollection<?> inputCollection;
        private final InputSchemaT inputSchema;
        private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final Selector<RuntimeSchemaT, T> selector;
        private final Map<String, String> renameMap;

        private Format(final DataType outputType,
                       final FCollection<?> inputCollection,
                       final InputSchemaT inputSchema,
                       final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                       final Selector<RuntimeSchemaT,T> selector,
                       final Map<String, String> renameMap) {

            this.outputType = outputType;
            this.inputCollection = inputCollection;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.selector = selector;
            this.renameMap = renameMap;
        }

        @Override
        public PCollection<T> expand(final PCollection<?> input) {
            final PCollection<T> formatted = input
                    .apply("ConvertTo" + outputType.name(), DataTypeTransform.transform(inputCollection, outputType));
            return formatted
                    .apply("SelectFields", ParDo.of(new SelectDoFn<>(inputSchema, schemaConverter, selector, renameMap)));
        }

        private static class SelectDoFn<T, InputSchemaT, RuntimeSchemaT> extends DoFn<T, T> {

            private final InputSchemaT inputSchema;
            private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final Selector<RuntimeSchemaT, T> selector;
            private final Map<String, String> renameFields;

            private transient RuntimeSchemaT schema;

            SelectDoFn(final InputSchemaT inputSchema,
                       final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                       final Selector<RuntimeSchemaT, T> selector,
                       final Map<String, String> renameFields) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.selector = selector;
                this.renameFields = renameFields;
            }

            @Setup
            public void setup() {
                this.schema = schemaConverter.convert(inputSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final T output = selector.convert(schema, element, renameFields);
                c.output(output);
            }
        }

    }

    private static DataType selectDataType(
            final UnionTransformParameters parameters,
            final List<FCollection<?>> inputs) {

        if(parameters.getOutputType() != null) {
            return parameters.getOutputType();
        }
        if(parameters.getBaseInput() != null) {
            return inputs.stream()
                    .filter(f -> f.getName().equals(parameters.getBaseInput()))
                    .map(FCollection::getDataType)
                    .findAny()
                    .orElseThrow();
        }
        return inputs.get(0).getDataType();
    }

    private static Schema createUnionSchema(final List<FCollection<?>> inputs, final UnionTransformParameters parameters) {

        if(parameters.getBaseInput() != null) {
            return inputs.stream()
                    .filter(i -> i.getName().equals(parameters.getBaseInput()))
                    .map(FCollection::getSchema)
                    .findAny()
                    .orElseThrow();
        } else if(parameters.getMappings().size() > 0) {
            final Schema.Builder builder = Schema.builder();
            for(final MappingParameter mapping : parameters.getMappings()) {
                final Schema.Field field = inputs.stream()
                        .filter(i -> i.getName().equals(mapping.getInputs().get(0).getInput()))
                        .map(FCollection::getSchema)
                        .map(s -> s.getField(mapping.getInputs().get(0).getField()))
                        .findAny()
                        .orElseThrow();
                builder.addField(mapping.getOutputField(), field.getType());
            }
            return builder.build();
        } else {
            final Set<String> fieldNames = new HashSet<>();
            final Schema.Builder builder = Schema.builder();
            for(final FCollection<?> input : inputs) {
                for(final Schema.Field field : input.getSchema().getFields()) {
                    if(!fieldNames.contains(field.getName())) {
                        builder.addField(field);
                        fieldNames.add(field.getName());
                    }
                }
            }
            return builder.build();
        }
    }

    private static Map<String, Map<String, String>> createRenameMap(final UnionTransformParameters parameters) {
        if(parameters.getMappings().size() == 0) {
            return new HashMap<>();
        } else {
            return parameters.getMappings().stream()
                    .flatMap(m -> m.getInputs().stream()
                            .map(i -> KV.of(i.getInput(), KV.of(i.getField(), m.getOutputField()))))
                    .collect(Collectors.groupingBy(KV::getKey))
                    .entrySet().stream()
                    .map(e -> KV.of(e.getKey(), e.getValue().stream()
                            .collect(Collectors.toMap(c -> c.getValue().getKey() , c -> c.getValue().getValue()))))
                    .collect(Collectors.toMap(KV::getKey, KV::getValue));
        }
    }

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(final InputSchemaT schema);
    }

    private interface Selector<SchemaT, T> extends Serializable {
        T convert(final SchemaT schema, final T element, final Map<String, String> renameFields);
    }


}
