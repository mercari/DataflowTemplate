package com.mercari.solution.module.transform;

import com.google.common.base.Functions;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class GroupByTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(GroupByTransform.class);

    private class GroupByTransformParameters {

        private List<String> keys;

        public List<String> getKeys() {
            return keys;
        }

        public void setKeys(List<String> keys) {
            this.keys = keys;
        }

    }

    public String getName() { return "groupby"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return Collections.singletonMap(config.getName(), GroupByTransform.transform(inputs, config));
    }

    public static FCollection<GenericRecord> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        PCollectionTuple groupbyInputs = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            groupbyInputs = groupbyInputs.and(input.getName(), input.getCollection());
        }

        final Map<String, FCollection<?>> inputCollections = inputs.stream()
                .collect(Collectors.toMap(FCollection::getName, Functions.identity()));

        final GroupBy transform = new GroupBy(config, inputCollections);
        final PCollection<GenericRecord> output = groupbyInputs.apply(config.getName(), transform);
        return FCollection.of(config.getName(), output, DataType.AVRO, transform.schema);
    }

    public static class GroupBy extends PTransform<PCollectionTuple, PCollection<GenericRecord>> {

        private final GroupByTransformParameters parameters;
        private final Map<String,FCollection<?>> inputCollections;

        private Schema schema;

        public GroupByTransformParameters getParameters() {
            return parameters;
        }

        private GroupBy(final TransformConfig config, final Map<String,FCollection<?>> inputCollections) {
            this.parameters = new Gson().fromJson(config.getParameters(), GroupByTransformParameters.class);
            this.inputCollections = inputCollections;
            validate();
        }

        @Override
        public PCollection<GenericRecord> expand(final PCollectionTuple tuple) {
            final List<String> tags = new ArrayList<>();
            KeyedPCollectionTuple<String> groupbyInputs = KeyedPCollectionTuple.empty(tuple.getPipeline());
            for(final Map.Entry<TupleTag<?>, PCollection<?>> input : tuple.getAll().entrySet()) {
                final FCollection<?> inputCollection = this.inputCollections.get(input.getKey().getId());
                final TupleTag<?> tag = input.getKey();
                final PCollection kv = input.getValue()
                        .apply("WithKeys", DataTypeTransform.withKeys(inputCollection, parameters.getKeys()));
                groupbyInputs = groupbyInputs.and(tag.getId(), kv);
                tags.add(tag.getId());
            }

            final Map<String, Schema> inputSchemas = inputCollections.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAvroSchema()));

            final List<KV<String, Schema>> keySchemas = inputSchemas.entrySet().stream()
                    .map(Map.Entry::getValue)
                    .map(s -> {
                        final List<KV<String, Schema>> schemas = new ArrayList<>();
                        for(final String keyField : parameters.getKeys()) {
                            schemas.add(KV.of(keyField, s.getField(keyField.trim()).schema()));
                        }
                        return schemas;
                    })
                    .findAny().orElseThrow(() -> new IllegalStateException(""));
            this.schema = createGroupAvroSchema(keySchemas, inputSchemas);

            final Map<String, DataType> inputTypes = inputCollections.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDataType()));
            final Map<String, String> inputSchemaStrings = inputSchemas.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

            return groupbyInputs
                    .apply("CoGroupByKey", CoGroupByKey.create())
                    .apply("AggregateOneRecord", ParDo.of(new GroupByDoFn(tags, inputTypes, inputSchemaStrings, this.schema.toString())))
                    .setCoder(AvroCoder.of(this.schema));
        }

        private void validate() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("GroupBy module parameter missing!");
            }
            if(this.parameters.getKeys() == null) {
                throw new IllegalArgumentException("GroupBy module required keys parameter!");
            }
            if(this.inputCollections == null || this.inputCollections.size() == 0) {
                throw new IllegalArgumentException("GroupBy module inputs size is zero!");
            }
        }

        private static Schema createGroupAvroSchema(final List<KV<String, Schema>> keySchemas, final Map<String, Schema> schemas) {
            final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
            for(final KV<String, Schema> keySchema : keySchemas) {
                schemaFields.name(keySchema.getKey()).type(keySchema.getValue()).noDefault();
            }
            for(final Map.Entry<String, Schema> schema : schemas.entrySet()) {
                final String name;
                if(schema.getKey().contains(".")) {
                    final String[] ns = schema.getKey().split("\\.");
                    name = ns[ns.length-1];
                } else {
                    name = schema.getKey();
                }
                final Schema childSchema = AvroSchemaUtil.toBuilder(schema.getValue(), schema.getKey(), null).endRecord();
                schemaFields.name(name).type(Schema.createArray(childSchema)).noDefault();
            }
            return schemaFields.endRecord();
        }


    }

    private static class GroupByDoFn extends DoFn<KV<String, CoGbkResult>, GenericRecord> {

        private final List<String> tags;

        private final Map<String, DataType> inputTypes;
        private final Map<String, String> inputSchemaStrings;
        private final String outputShemaString;

        private transient Map<String, Schema> inputSchemas;
        private transient Schema outputSchema;

        public GroupByDoFn(final List<String> tags,
                           final Map<String, DataType> inputTypes,
                           final Map<String,String> inputSchemaStrings,
                           final String outputShemaString) {
            this.tags = tags;
            this.inputTypes = inputTypes;
            this.inputSchemaStrings = inputSchemaStrings;
            this.outputShemaString = outputShemaString;
        }

        @Setup
        public void setup() {
            this.inputSchemas = inputSchemaStrings.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new Schema.Parser().parse(e.getValue())));
            this.outputSchema = new Schema.Parser().parse(this.outputShemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final CoGbkResult result = c.element().getValue();
            if(result.isEmpty()) {
                return;
            }
            final Map<String, Iterable<GenericRecord>> recordsMap = new HashMap<>();
            GenericRecord sampleRecord = null;
            for(final String tableName : tags) {
                final Schema inputSchema = inputSchemas.get(tableName);
                final List<GenericRecord> records = new ArrayList<>();
                for(final Object value : result.getAll(tableName)) {
                    final GenericRecord record = DataTypeTransform.convertRecord(inputTypes.get(tableName), inputSchema, value);
                    records.add(record);
                    if(sampleRecord == null) {
                        sampleRecord = record;
                    }
                }
                if(tableName.contains(".")) {
                    final String[] ns = tableName.split("\\.");
                    recordsMap.put(ns[ns.length-1], records);
                } else {
                    recordsMap.put(tableName, records);
                }
            }
            final GenericRecord record = createGroupAvroRecord(outputSchema, recordsMap, sampleRecord);
            c.output(record);
        }

        private static GenericRecord createGroupAvroRecord(
                final Schema schema, final Map<String, Iterable<GenericRecord>> records, final GenericRecord sampleRecord) {
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            for(final Schema.Field field : schema.getFields()) {
                if(records.containsKey(field.name())) {
                    builder.set(field, records.get(field.name()));
                } else {
                    builder.set(field, sampleRecord.get(field.name()));
                }
            }
            return builder.build();
        }

    }

}
