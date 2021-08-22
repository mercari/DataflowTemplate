package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class FilterTransform implements TransformModule {

    private class FilterTransformParameters {

        private List<String> fields;
        private JsonElement filters;
        private Map<String, String> renameFields;

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public JsonElement getFilters() {
            return filters;
        }

        public void setFilters(JsonElement filters) {
            this.filters = filters;
        }

        public Map<String, String> getRenameFields() {
            return renameFields;
        }

        public void setRenameFields(Map<String, String> renameFields) {
            this.renameFields = renameFields;
        }
    }

    @Override
    public String getName() { return "filter"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return FilterTransform.transform(inputs, config);
    }

    static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final FilterTransformParameters parameters = new Gson().fromJson(config.getParameters(), FilterTransformParameters.class);

        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    Schema schema;
                    if (parameters.getFields().size() == 0) {
                        schema = inputCollection.getSchema();
                    } else {
                        schema = RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    }
                    if (parameters.getRenameFields().size() > 0) {
                        final List<Schema.Field> rf = schema.getFields().stream()
                                .filter(f -> parameters.getRenameFields().containsKey(f.getName()))
                                .map(f -> f.toBuilder().setName(parameters.getRenameFields().get(f.getName())).build())
                                .collect(Collectors.toList());
                        schema = RowSchemaUtil.removeFields(schema, parameters.getRenameFields().keySet());
                        schema = RowSchemaUtil.addSchema(schema, rf);
                    }
                    final Transform<Row, Schema, Schema> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            RowSchemaUtil::getValue,
                            (Schema s, Row r, Map<String, String> rf) -> RowSchemaUtil.toBuilder(s, r, rf).build());
                    final PCollection<Row> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(RowCoder.of(schema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, schema));
                    break;
                }
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    org.apache.avro.Schema schema;
                    if (parameters.getFields().size() == 0) {
                        schema = inputCollection.getAvroSchema();
                    } else {
                        schema = AvroSchemaUtil.selectFields(inputCollection.getAvroSchema(), parameters.getFields());
                    }
                    if (parameters.getRenameFields().size() > 0) {
                        schema = AvroSchemaUtil.renameFields(schema, parameters.getRenameFields());
                    }
                    final Transform<GenericRecord, String, org.apache.avro.Schema> transform = new Transform<>(
                            parameters,
                            schema.toString(),
                            AvroSchemaUtil::convertSchema,
                            AvroSchemaUtil::getValue,
                            (org.apache.avro.Schema s, GenericRecord r, Map<String, String> rf) -> AvroSchemaUtil.toBuilder(s, r, rf).build());
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(schema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, schema));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    Type type;
                    if (parameters.getFields().size() == 0) {
                        type = inputCollection.getSpannerType();
                    } else {
                        type = StructSchemaUtil.selectFields(inputCollection.getSpannerType(), parameters.getFields());
                    }
                    if (parameters.getRenameFields().size() > 0) {
                        type = StructSchemaUtil.renameFields(type, parameters.getRenameFields());
                    }
                    final Transform<Struct, Type, Type> transform = new Transform<>(
                            parameters,
                            type,
                            s -> s,
                            StructSchemaUtil::getValue,
                            (Type t, Struct s, Map<String, String> rf) -> StructSchemaUtil.toBuilder(t, s, rf).build());
                    final PCollection<Struct> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, type));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    Schema schema;
                    if (parameters.getFields().size() == 0) {
                        schema = inputCollection.getSchema();
                    } else {
                        schema = RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    }
                    if (parameters.getRenameFields().size() > 0) {
                        final List<Schema.Field> rf = schema.getFields().stream()
                                .filter(f -> parameters.getRenameFields().containsKey(f.getName()))
                                .map(f -> f.toBuilder().setName(parameters.getRenameFields().get(f.getName())).build())
                                .collect(Collectors.toList());
                        schema = RowSchemaUtil.removeFields(schema, parameters.getRenameFields().keySet());
                        schema = RowSchemaUtil.addSchema(schema, rf);
                    }
                    final Transform<Entity, Schema, Schema> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            EntitySchemaUtil::getValue,
                            (Schema s, Entity e, Map<String, String> rf) -> EntitySchemaUtil.toBuilder(s, e, rf).build());
                    final PCollection<Entity> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.ENTITY, schema));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Filter transform module does not support input data type: " + input.getDataType());
            }
        }

        return results;
    }

    private static void validateParameters(final FilterTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("Filter transform module parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getFilters() == null && parameters.getFields() == null) {
            errorMessages.add("Filter transform module parameters must contain filters or fields parameter.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(FilterTransformParameters parameters) {
        if(parameters.getFields() == null) {
            parameters.setFields(new ArrayList<>());
        }
        if(parameters.getRenameFields() == null) {
            parameters.setRenameFields(new HashMap<>());
        }
    }

    public static class Transform<T, InputSchemaT, RuntimeSchemaT> extends PTransform<PCollection<T>, PCollection<T>> {

        private final FilterTransformParameters parameters;
        private final InputSchemaT inputSchema;
        private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final Filter.Getter<T> getter;
        private final Selector<T, RuntimeSchemaT> selector;

        private Transform(final FilterTransformParameters parameters,
                          final InputSchemaT inputSchema,
                          final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                          final Filter.Getter<T> getter,
                          final Selector<T, RuntimeSchemaT> selector) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.getter = getter;
            this.selector = selector;
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {

            final PCollection<T> filtered = parameters.getFilters() == null ? input : input.apply("FilterRows", ParDo
                    .of(new FilterDoFn<>(parameters.getFilters().toString(), getter)));

            if(parameters.getFields().size() == 0 && parameters.getRenameFields().size() == 0) {
                return filtered;
            }

            final Map<String, String> reversedRenameFields = parameters.getRenameFields().entrySet()
                    .stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

            return filtered.apply("SelectFields", ParDo
                    .of(new SelectDoFn<>(inputSchema, schemaConverter, selector, reversedRenameFields)));
        }

        private static class FilterDoFn<T> extends DoFn<T, T> {

            private final String conditionJsons;
            private final Filter.Getter<T> getter;

            private transient Filter.ConditionNode conditions;

            FilterDoFn(final String conditionJsons,
                       final Filter.Getter<T> getter) {

                this.conditionJsons = conditionJsons;
                this.getter = getter;
            }

            @Setup
            public void setup() {
                this.conditions = Filter.parse(new Gson().fromJson(conditionJsons, JsonElement.class));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                if(Filter.filter(element, getter, conditions)) {
                    c.output(element);
                }
            }
        }

        private static class SelectDoFn<T, InputSchemaT, RuntimeSchemaT> extends DoFn<T, T> {

            private final InputSchemaT inputSchema;
            private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final Selector<T, RuntimeSchemaT> selector;
            private final Map<String, String> renameFields;

            private transient RuntimeSchemaT schema;

            SelectDoFn(final InputSchemaT inputSchema,
                       final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                       final Selector<T, RuntimeSchemaT> selector,
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

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(final InputSchemaT schema);
    }

    private interface Selector<T, SchemaT> extends Serializable {
        T convert(final SchemaT schema, final T element, final Map<String, String> renameFields);
    }

}
