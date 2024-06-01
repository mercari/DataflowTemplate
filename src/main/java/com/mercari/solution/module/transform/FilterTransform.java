package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.converter.RowToMutationConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class FilterTransform implements TransformModule {

    private static class FilterTransformParameters {

        private JsonElement filters;
        private JsonArray select;
        private List<String> fields;
        private Map<String, String> renameFields;

        public JsonElement getFilters() {
            return filters;
        }

        public JsonArray getSelect() {
            return select;
        }

        public List<String> getFields() {
            return fields;
        }

        public Map<String, String> getRenameFields() {
            return renameFields;
        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if((this.filters == null || this.filters.isJsonNull())
                    && (this.select == null || !this.select.isJsonArray())
                    && this.fields == null) {

                errorMessages.add("Filter transform module parameters must contain filters, select or fields parameter.");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        private void setDefaults() {
            if(this.fields == null) {
                this.fields = new ArrayList<>();
            }
            if(this.renameFields == null) {
                this.renameFields = new HashMap<>();
            }
        }

        public static FilterTransformParameters of(final TransformConfig config) {
            final FilterTransformParameters parameters = new Gson().fromJson(config.getParameters(), FilterTransformParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("Filter transform module " + config.getName() + " parameters must not be empty!");
            }
            parameters.validate();
            parameters.setDefaults();
            return parameters;
        }
    }

    @Override
    public String getName() { return "filter"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return FilterTransform.transform(inputs, config);
    }

    static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final FilterTransformParameters parameters = FilterTransformParameters.of(config);

        final boolean useSelect = parameters.getSelect() != null && parameters.getSelect().isJsonArray();
        final boolean useFields = !parameters.getFields().isEmpty();

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());

            final DataType outputType = input.getDataType();
            final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.getSelect(), input.getSchema().getFields(), outputType);

            switch (input.getDataType()) {
                case ROW -> {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    Schema schema;
                    if(useSelect) {
                        schema = SelectFunction.createSchema(selectFunctions);
                    } else if(useFields) {
                        schema = RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    } else {
                        schema = inputCollection.getSchema();
                    }
                    if(!parameters.getRenameFields().isEmpty()) {
                        final List<Schema.Field> rf = schema.getFields().stream()
                                .filter(f -> parameters.getRenameFields().containsKey(f.getName()))
                                .map(f -> f.toBuilder().setName(parameters.getRenameFields().get(f.getName())).build())
                                .collect(Collectors.toList());
                        schema = RowSchemaUtil.removeFields(schema, parameters.getRenameFields().keySet());
                        schema = RowSchemaUtil.addSchema(schema, rf);
                    }
                    final Transform<Schema, Schema, Row> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            RowSchemaUtil::getValue,
                            RowSchemaUtil::create,
                            outputType,
                            selectFunctions,
                            (Schema s, Row r, Map<String, String> rf) -> RowSchemaUtil.toBuilder(s, r, rf).build());
                    final PCollection<Row> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(RowCoder.of(schema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, schema));
                }
                case AVRO -> {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    org.apache.avro.Schema schema;
                    if(useSelect) {
                        schema = RowToRecordConverter.convertSchema(SelectFunction.createSchema(selectFunctions));
                    } else if(useFields) {
                        schema = AvroSchemaUtil.selectFields(inputCollection.getAvroSchema(), parameters.getFields());
                    } else {
                        schema = inputCollection.getAvroSchema();
                    }
                    if (!parameters.getRenameFields().isEmpty()) {
                        schema = AvroSchemaUtil.renameFields(schema, parameters.getRenameFields());
                    }
                    final Transform<String, org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                            parameters,
                            schema.toString(),
                            AvroSchemaUtil::convertSchema,
                            AvroSchemaUtil::getValue,
                            AvroSchemaUtil::create,
                            outputType,
                            selectFunctions,
                            (org.apache.avro.Schema s, GenericRecord r, Map<String, String> rf) -> AvroSchemaUtil.toBuilder(s, r, rf).build());
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(schema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, schema));
                }
                case STRUCT -> {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    Type type;
                    if(useSelect) {
                        type = RowToMutationConverter.convertSchema(SelectFunction.createSchema(selectFunctions));
                    } else if(useFields) {
                        type = StructSchemaUtil.selectFields(inputCollection.getSpannerType(), parameters.getFields());
                    } else {
                        type = inputCollection.getSpannerType();
                    }
                    if (!parameters.getRenameFields().isEmpty()) {
                        type = StructSchemaUtil.renameFields(type, parameters.getRenameFields());
                    }
                    final Transform<Type, Type, Struct> transform = new Transform<>(
                            parameters,
                            type,
                            s -> s,
                            StructSchemaUtil::getValue,
                            StructSchemaUtil::create,
                            outputType,
                            selectFunctions,
                            (Type t, Struct s, Map<String, String> rf) -> StructSchemaUtil.toBuilder(t, s, rf).build());
                    final PCollection<Struct> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, type));
                }
                case DOCUMENT -> {
                    final FCollection<Document> inputCollection = (FCollection<Document>) input;
                    Schema schema;
                    if(useSelect) {
                        schema = SelectFunction.createSchema(selectFunctions);
                    } else if(useFields) {
                        schema = RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    } else {
                        schema = inputCollection.getSchema();
                    }
                    if (!parameters.getRenameFields().isEmpty()) {
                        final List<Schema.Field> rf = schema.getFields().stream()
                                .filter(f -> parameters.getRenameFields().containsKey(f.getName()))
                                .map(f -> f.toBuilder().setName(parameters.getRenameFields().get(f.getName())).build())
                                .collect(Collectors.toList());
                        schema = RowSchemaUtil.removeFields(schema, parameters.getRenameFields().keySet());
                        schema = RowSchemaUtil.addSchema(schema, rf);
                    }
                    final Transform<Schema, Schema, Document> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            DocumentSchemaUtil::getValue,
                            DocumentSchemaUtil::create,
                            outputType,
                            selectFunctions,
                            (Schema s, Document e, Map<String, String> rf) -> DocumentSchemaUtil.toBuilder(s, e, rf).build());
                    final PCollection<Document> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.DOCUMENT, schema));
                }
                case ENTITY -> {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    Schema schema;
                    if(useSelect) {
                        schema = SelectFunction.createSchema(selectFunctions);
                    } else if(useFields) {
                        schema = RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    } else {
                        schema = inputCollection.getSchema();
                    }
                    if (!parameters.getRenameFields().isEmpty()) {
                        final List<Schema.Field> rf = schema.getFields().stream()
                                .filter(f -> parameters.getRenameFields().containsKey(f.getName()))
                                .map(f -> f.toBuilder().setName(parameters.getRenameFields().get(f.getName())).build())
                                .collect(Collectors.toList());
                        schema = RowSchemaUtil.removeFields(schema, parameters.getRenameFields().keySet());
                        schema = RowSchemaUtil.addSchema(schema, rf);
                    }
                    final Transform<Schema, Schema, Entity> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            EntitySchemaUtil::getValue,
                            EntitySchemaUtil::create,
                            outputType,
                            selectFunctions,
                            (Schema s, Entity e, Map<String, String> rf) -> EntitySchemaUtil.toBuilder(s, e, rf).build());
                    final PCollection<Entity> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.ENTITY, schema));
                }
                default -> throw new IllegalArgumentException("Filter transform module does not support input data type: " + input.getDataType());
            }
        }

        return results;
    }

    public static class Transform<InputSchemaT, RuntimeSchemaT, T> extends PTransform<PCollection<T>, PCollection<T>> {

        private final FilterTransformParameters parameters;
        private final InputSchemaT inputSchema;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.ValueGetter<T> valueGetter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator;
        private final Selector<RuntimeSchemaT, T> selector;
        private final DataType inputType;
        private final List<SelectFunction> selectFunctions;

        private Transform(final FilterTransformParameters parameters,
                          final InputSchemaT inputSchema,
                          final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                          final SchemaUtil.ValueGetter<T> valueGetter,
                          final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                          final DataType inputType,
                          final List<SelectFunction> selectFunctions,
                          final Selector<RuntimeSchemaT, T> selector) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.valueGetter = valueGetter;
            this.valueCreator = valueCreator;
            this.inputType = inputType;
            this.selectFunctions = selectFunctions;
            this.selector = selector;
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {

            final PCollection<T> filtered;
            if(parameters.getFilters() == null || parameters.getFilters().isJsonNull()) {
                filtered = input;
            } else {
                filtered = input
                        .apply("Filter", ParDo.of(
                                new FilterDoFn<>(parameters.getFilters().toString(), valueGetter)));
            }

            if((parameters.getSelect() == null || parameters.getSelect().isEmpty())
                    && (parameters.getFields().isEmpty() && parameters.getRenameFields().isEmpty())) {
                return filtered;
            }

            if(!parameters.getFields().isEmpty() || !parameters.getRenameFields().isEmpty()) {
                final Map<String, String> reversedRenameFields = parameters.getRenameFields().entrySet()
                        .stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                return filtered.apply("Fields", ParDo
                        .of(new FieldsDoFn<>(inputSchema, schemaConverter, selector, reversedRenameFields)));
            } else {
                return filtered.apply("Select", ParDo
                        .of(new SelectDoFn<>(inputSchema, schemaConverter, valueCreator, inputType, inputType, selectFunctions)));
            }

        }

        private static class FilterDoFn<T> extends DoFn<T, T> {

            private final String conditionJsons;
            private final SchemaUtil.ValueGetter<T> valueGetter;

            private transient Filter.ConditionNode conditions;

            FilterDoFn(final String conditionJsons,
                       final SchemaUtil.ValueGetter<T> valueGetter) {

                this.conditionJsons = conditionJsons;
                this.valueGetter = valueGetter;
            }

            @Setup
            public void setup() {
                this.conditions = Filter.parse(new Gson().fromJson(conditionJsons, JsonElement.class));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                if(Filter.filter(element, valueGetter, conditions)) {
                    c.output(element);
                }
            }
        }

        private static class SelectDoFn<InputSchemaT, RuntimeSchemaT, T> extends DoFn<T, T> {

            private final InputSchemaT inputSchema;
            private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator;
            private final DataType inputType;
            private final DataType outputType;
            private final List<SelectFunction> selectFunctions;

            private transient RuntimeSchemaT schema;

            SelectDoFn(final InputSchemaT inputSchema,
                       final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                       final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                       final DataType inputType,
                       final DataType outputType,
                       final List<SelectFunction> selectFunctions) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.valueCreator = valueCreator;
                this.inputType = inputType;
                this.outputType = outputType;
                this.selectFunctions = selectFunctions;
            }

            @Setup
            public void setup() {
                this.schema = schemaConverter.convert(inputSchema);
                for(SelectFunction selectFunction: selectFunctions) {
                    selectFunction.setup();
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final Map<String, Object> values = SelectFunction.apply(selectFunctions, element, inputType, outputType, c.timestamp());
                final T output = valueCreator.create(schema, values);
                c.output(output);
            }
        }

        private static class FieldsDoFn<InputSchemaT, RuntimeSchemaT, T> extends DoFn<T, T> {

            private final InputSchemaT inputSchema;
            private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final Selector<RuntimeSchemaT, T> selector;
            private final Map<String, String> renameFields;

            private transient RuntimeSchemaT schema;

            FieldsDoFn(final InputSchemaT inputSchema,
                       final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
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

    private interface Selector<SchemaT, T> extends Serializable {
        T convert(final SchemaT schema, final T element, final Map<String, String> renameFields);
    }

}
