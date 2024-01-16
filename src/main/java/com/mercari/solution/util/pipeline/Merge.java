package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Merge<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PCollectionTuple, PCollection<T>> {

    public static final String DEFAULT_FIELD_NAME_SOURCE = "__source";
    public static final String DEFAULT_FIELD_NAME_TIMESTAMP = "__epochmillis";

    private static final Logger LOG = LoggerFactory.getLogger(Merge.class);

    private final String name;
    private final List<String> commonFields;
    private final Map<TupleTag<?>, String> inputNames;
    private final Map<TupleTag<?>, DataType> inputTypes;
    private final Map<TupleTag<?>, InputSchemaT> inputSchemas;
    private final DataType outputType;

    private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
    private final SchemaUtil.ValueGetter<T> valueGetter;
    private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;


    public Merge(final String name,
                 final List<String> commonFields,
                 final Map<TupleTag<?>, String> inputNames,
                 final Map<TupleTag<?>, DataType> inputTypes,
                 final Map<TupleTag<?>, InputSchemaT> inputSchemas,
                 final DataType outputType,
                 final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                 final SchemaUtil.ValueGetter<T> valueGetter,
                 final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator) {

        this.name = name;
        this.commonFields = commonFields;
        this.inputNames = inputNames;
        this.inputTypes = inputTypes;
        this.inputSchemas = inputSchemas;
        this.outputType = outputType;

        this.schemaConverter = schemaConverter;
        this.valueGetter = valueGetter;
        this.valueCreator = valueCreator;
    }


    @Override
    public PCollection<T> expand(PCollectionTuple inputs) {
        final InputSchemaT outputSchema = createOutputSchema();
        final RuntimeSchemaT outputRuntimeSchema = schemaConverter.convert(outputSchema);
        final Coder<T> outputCoder = createOutputCoder(outputRuntimeSchema);

        final List<String> sourceNames = new ArrayList<>(inputNames.values());

        PCollectionList<T> list = PCollectionList.empty(inputs.getPipeline());
        for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : inputs.getAll().entrySet()) {
            final TupleTag<?> tag = entry.getKey();
            final String inputName = inputNames.get(tag);
            if(!inputTypes.containsKey(tag) || !inputSchemas.containsKey(tag)) {
                continue;
            }

            final PCollection<T> input;
            if(outputType.equals(inputTypes.get(tag))) {
                input = (PCollection<T>) entry.getValue();
            } else {
                final RuntimeSchemaT runtimeSchema = schemaConverter.convert(inputSchemas.get(tag));
                input = (PCollection<T>) (entry.getValue().apply("ConvertType" + inputName, SchemaUtil.transform(runtimeSchema, inputTypes.get(tag), outputType)));
            }

            final PCollection<T> unified = input
                    .apply("Merge" + inputName, ParDo.of(new MergeDoFn<>(inputName, commonFields, sourceNames, outputSchema, schemaConverter, valueGetter, valueCreator)))
                    .setCoder(outputCoder);

            list = list.and(unified);
        }

        return list.apply("Union", Flatten.pCollections());
    }

    private static class MergeDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<T, T> {

        private final String source;
        private final List<String> commonFields;
        private final List<String> sourceNames;
        protected final InputSchemaT outputSchema;
        protected final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        protected final SchemaUtil.ValueGetter<T> valueGetter;
        protected final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;


        protected transient RuntimeSchemaT runtimeOutputSchema;

        MergeDoFn(final String source,
                  final List<String> commonFields,
                  final List<String> sourceNames,
                  final InputSchemaT outputSchema,
                  final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.ValueGetter<T> valueGetter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator) {

            this.source = source;
            this.commonFields = commonFields;
            this.sourceNames = sourceNames;
            this.outputSchema = outputSchema;
            this.schemaConverter = schemaConverter;
            this.valueGetter = valueGetter;
            this.valueCreator = valueCreator;
        }

        @Setup
        public void setup() {
            this.runtimeOutputSchema = schemaConverter.convert(outputSchema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final T element = c.element();

            final Map<String, Object> values = new HashMap<>();
            values.put(DEFAULT_FIELD_NAME_SOURCE, source);
            values.put(DEFAULT_FIELD_NAME_TIMESTAMP, c.timestamp().getMillis());
            for(final String commonField : commonFields) {
                values.put(commonField, valueGetter.getValue(element, commonField));
            }

            for(final String sourceName : sourceNames) {
                final List<T> elements = new ArrayList<>();
                if(source.equals(sourceName)) {
                    elements.add(element);
                }
                values.put(sourceName, elements);
            }

            if(true) {
                //throw new IllegalArgumentException("schema: " + runtimeOutputSchema.toString() + ", values: " + values);
            }

            final T output = valueCreator.create(runtimeOutputSchema, values);

            c.output(output);
        }

    }

    private InputSchemaT createOutputSchema() {
        switch (outputType) {
            case ROW:
                return (InputSchemaT) createOutputRowSchema();
            case AVRO:
                return (InputSchemaT) (createOutputAvroSchema(this.name).toString());
            default:
                throw new IllegalArgumentException();

        }
    }

    private Coder createOutputCoder(final RuntimeSchemaT runtimeOutputSchema) {
        switch (outputType) {
            case ROW:
                return RowCoder.of((Schema)runtimeOutputSchema);
            case AVRO:
                return AvroCoder.of((org.apache.avro.Schema)runtimeOutputSchema);
            default:
                throw new IllegalArgumentException();

        }
    }

    private Schema createOutputRowSchema() {
        final Schema.Builder builder = Schema.builder()
                .addField(DEFAULT_FIELD_NAME_SOURCE, Schema.FieldType.STRING)
                .addField(DEFAULT_FIELD_NAME_TIMESTAMP, Schema.FieldType.INT64);

        final Schema sampleSchema = (Schema) inputSchemas.values().stream().findFirst().orElseThrow();
        for(final String fieldName : commonFields) {
            if(!sampleSchema.hasField(fieldName)) {
                throw new IllegalArgumentException("Not found common field: " + fieldName + " in schema: " + sampleSchema);
            }
            builder.addField(fieldName, sampleSchema.getField(fieldName).getType().withNullable(true));
        }

        for(final Map.Entry<TupleTag<?>, InputSchemaT> entry : inputSchemas.entrySet()) {
            final Schema fieldSchema = (Schema) entry.getValue();
            final String fieldName = inputNames.get(entry.getKey());
            builder.addField(fieldName, Schema.FieldType.array(Schema.FieldType.row(fieldSchema)));
        }
        return builder.build();
    }

    private org.apache.avro.Schema createOutputAvroSchema(final String name) {
        final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> schemaFields = SchemaBuilder.record(name).fields()
                .name(DEFAULT_FIELD_NAME_SOURCE).type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .name(DEFAULT_FIELD_NAME_TIMESTAMP).type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)).noDefault();

        final org.apache.avro.Schema sampleSchema = AvroSchemaUtil.convertSchema((String)(inputSchemas.values().stream().findFirst().orElseThrow()));
        for(final String fieldName : commonFields) {
            final org.apache.avro.Schema.Field field = sampleSchema.getField(fieldName);
            if(field == null) {
                throw new IllegalArgumentException("Not found common field: " + fieldName + " in schema: " + sampleSchema);
            }
            schemaFields.name(fieldName).type(AvroSchemaUtil.toNullable(field.schema())).noDefault();
        }

        for(final Map.Entry<TupleTag<?>, InputSchemaT> entry : inputSchemas.entrySet()) {
            final String fieldName = inputNames.get(entry.getKey());
            final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.convertSchema((String) entry.getValue());
            final org.apache.avro.Schema mergedSchema = org.apache.avro.Schema
                    .createArray(AvroSchemaUtil.rename(AvroSchemaUtil.unnestUnion(fieldSchema), fieldName));
            schemaFields
                    .name(fieldName)
                    .type(mergedSchema)
                    .noDefault();
        }
        return schemaFields.endRecord();
    }
}

