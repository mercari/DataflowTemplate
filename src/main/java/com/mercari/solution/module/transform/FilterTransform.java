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


public class FilterTransform implements TransformModule {

    private class FilterTransformParameters {

        private List<String> fields;
        private JsonElement filters;

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
                    final Schema schema = parameters.getFields().size() == 0 ? inputCollection.getSchema()
                            : RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    final Transform<Row, Schema, Schema> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            RowSchemaUtil::getValue,
                            (Schema s, Row r) -> RowSchemaUtil.toBuilder(s, r).build());
                    final PCollection<Row> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(RowCoder.of(schema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, schema));
                    break;
                }
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final org.apache.avro.Schema schema = parameters.getFields().size() == 0 ? inputCollection.getAvroSchema()
                            : AvroSchemaUtil.selectFields(inputCollection.getAvroSchema(), parameters.getFields());
                    final Transform<GenericRecord, String, org.apache.avro.Schema> transform = new Transform<>(
                            parameters,
                            schema.toString(),
                            AvroSchemaUtil::convertSchema,
                            AvroSchemaUtil::getValue,
                            (org.apache.avro.Schema s, GenericRecord r) -> AvroSchemaUtil.toBuilder(s, r).build());
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(schema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, schema));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Type type = parameters.getFields().size() == 0 ? inputCollection.getSpannerType()
                            : StructSchemaUtil.selectFields(inputCollection.getSpannerType(), parameters.getFields());
                    final Transform<Struct, Type, Type> transform = new Transform<>(
                            parameters,
                            type,
                            s -> s,
                            StructSchemaUtil::getValue,
                            (Type t, Struct s) -> StructSchemaUtil.toBuilder(t, s).build());
                    final PCollection<Struct> output = inputCollection.getCollection()
                            .apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, type));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Schema schema = parameters.getFields().size() == 0 ? inputCollection.getSchema()
                            : RowSchemaUtil.selectFields(inputCollection.getSchema(), parameters.getFields());
                    final Transform<Entity, Schema, Schema> transform = new Transform<>(
                            parameters,
                            schema,
                            s -> s,
                            EntitySchemaUtil::getValue,
                            (Schema s, Entity e) -> EntitySchemaUtil.toBuilder(s, e).build());
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

            if(parameters.getFields().size() == 0) {
                return filtered;
            }

            return filtered.apply("SelectFields", ParDo
                    .of(new SelectDoFn<>(inputSchema, schemaConverter, selector)));
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

            private transient RuntimeSchemaT schema;

            SelectDoFn(final InputSchemaT inputSchema,
                       final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                       final Selector<T, RuntimeSchemaT> selector) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.selector = selector;
            }

            @Setup
            public void setup() {
                this.schema = schemaConverter.convert(inputSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final T output = selector.convert(schema, element);
                c.output(output);
            }
        }

    }

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(final InputSchemaT schema);
    }

    private interface Selector<T, SchemaT> extends Serializable {
        T convert(final SchemaT schema, final T element);
    }

}
