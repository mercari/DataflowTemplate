package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.domain.text.analyzer.TokenAnalyzer;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.tokenattributes.BaseFormAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.InflectionAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;


public class TokenizeTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(TokenizeTransform.class);

    private class TokenizeTransformParameters implements Serializable {

        private List<TokenizeParameter> fields;

        public List<TokenizeParameter> getFields() {
            return fields;
        }

        public void setFields(List<TokenizeParameter> fields) {
            this.fields = fields;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (this.getFields() == null) {
                errorMessages.add("TokenizeTransform config parameters must contain fields parameter.");
            } else if (this.getFields().size() == 0) {
                errorMessages.add("TokenizeTransform fields parameter size must be greater than zero.");
            } else {
                for(int i=0; i<fields.size(); i++) {
                    if(fields.get(i) == null) {
                        errorMessages.add("TokenizeTransform fields[" + i + "] parameter must not be null.");
                    } else {
                        errorMessages.addAll(fields.get(i).validate(i));
                    }
                }
            }

            if (errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            for(TokenizeParameter field : fields) {
                field.setDefault();
            }
        }

        private class TokenizeParameter implements Serializable {

            private String name;
            private String input;
            private List<TokenAnalyzer.CharFilterConfig> charFilters;
            private TokenAnalyzer.TokenizerConfig tokenizer;
            private List<TokenAnalyzer.TokenFilterConfig> filters;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getInput() {
                return input;
            }

            public void setInput(String input) {
                this.input = input;
            }

            public List<TokenAnalyzer.CharFilterConfig> getCharFilters() {
                return charFilters;
            }

            public void setCharFilters(List<TokenAnalyzer.CharFilterConfig> charFilters) {
                this.charFilters = charFilters;
            }

            public TokenAnalyzer.TokenizerConfig getTokenizer() {
                return tokenizer;
            }

            public void setTokenizer(TokenAnalyzer.TokenizerConfig tokenizer) {
                this.tokenizer = tokenizer;
            }

            public List<TokenAnalyzer.TokenFilterConfig> getFilters() {
                return filters;
            }

            public void setFilters(List<TokenAnalyzer.TokenFilterConfig> filters) {
                this.filters = filters;
            }

            public List<String> validate(int n) {
                final List<String> errorMessages = new ArrayList<>();
                if (this.name == null) {
                    errorMessages.add("TokenizeTransform parameters.fields[" + n + "].name must not be null.");
                }
                if (this.input == null) {
                    errorMessages.add("TokenizeTransform parameters.fields[" + n + "].input must not be null.");
                }
                if (this.tokenizer == null) {
                    errorMessages.add("TokenizeTransform parameters.fields[" + n + "].tokenizer must not be null.");
                } else {
                    errorMessages.addAll(this.tokenizer.validate());
                }
                if (this.charFilters != null) {
                    for(final TokenAnalyzer.CharFilterConfig filter : this.charFilters) {
                        if(filter == null) {
                            errorMessages.add("CharFilter must not be null");
                        } else {
                            errorMessages.addAll(filter.validate());
                        }
                    }
                }
                if (this.filters != null) {
                    for(final TokenAnalyzer.TokenFilterConfig filter : this.filters) {
                        if(filter == null) {
                            errorMessages.add("TokenFilter must not be null");
                        } else {
                            errorMessages.addAll(filter.validate());
                        }
                    }
                }

                return errorMessages;
            }

            public void setDefault() {
                this.tokenizer.setDefault();

                if(this.charFilters == null) {
                    this.charFilters = new ArrayList<>();
                } else {
                    for(final TokenAnalyzer.CharFilterConfig filter : charFilters) {
                        filter.setDefaults();
                    }
                }

                if(this.filters == null) {
                    this.filters = new ArrayList<>();
                } else {
                    for(final TokenAnalyzer.TokenFilterConfig filter : filters) {
                        filter.setDefaults();
                    }
                }
            }

        }

    }

    public String getName() {
        return "tokenize";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(List<FCollection<?>> inputs, TransformConfig config) {
        final TokenizeTransformParameters parameters = new Gson().fromJson(config.getParameters(), TokenizeTransformParameters.class);
        if (parameters == null) {
            throw new IllegalArgumentException("TokenizeTransform config parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final Map<String, FCollection<?>> results = new HashMap<>();
        for (final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Schema.Builder builder = RowSchemaUtil.toBuilder(input.getSchema());
                    final Map<String, Schema> inputFieldSchemas = new HashMap<>();
                    for(final TokenizeTransformParameters.TokenizeParameter field : parameters.getFields()) {
                        final Schema fieldSchema = createOutputSchema(field);
                        builder.addField(field.getName(), Schema.FieldType.array(Schema.FieldType.row(fieldSchema)).withNullable(true));
                        inputFieldSchemas.put(field.getName(), fieldSchema);
                    }
                    final Schema outputSchema = builder.build();
                    final Transform<Row, Schema, Schema> transform = new Transform<>(
                            parameters,
                            outputSchema,
                            inputFieldSchemas,
                            s -> s,
                            RowSchemaUtil::getAsString,
                            RowSchemaUtil::merge,
                            RowSchemaUtil::create);
                    final PCollection<Row> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(RowCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, outputSchema));
                    break;
                }
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = AvroSchemaUtil.toBuilder(input.getAvroSchema());
                    final Map<String, String> inputFieldSchemas = new HashMap<>();
                    for(final TokenizeTransformParameters.TokenizeParameter field : parameters.getFields()) {
                        final org.apache.avro.Schema fieldSchema = createOutputAvroSchema(field);
                        builder.name(field.getName())
                                .type(org.apache.avro.Schema.createArray(fieldSchema))
                                .noDefault();
                        inputFieldSchemas.put(field.getName(), fieldSchema.toString());
                    }
                    final org.apache.avro.Schema outputSchema = builder.endRecord();
                    final Transform<GenericRecord, String, org.apache.avro.Schema> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            inputFieldSchemas,
                            AvroSchemaUtil::convertSchema,
                            AvroSchemaUtil::getAsString,
                            AvroSchemaUtil::merge,
                            AvroSchemaUtil::create);
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final List<Type.StructField> fields = new ArrayList<>(input.getSpannerType().getStructFields());
                    final Map<String, Type> inputFieldSchemas = new HashMap<>();
                    for(final TokenizeTransformParameters.TokenizeParameter field : parameters.getFields()) {
                        final Type fieldType = createOutputType(field);
                        fields.add(Type.StructField.of(field.getName(), Type.array(fieldType)));
                        inputFieldSchemas.put(field.getName(), fieldType);
                    }
                    final Type outputType = Type.struct(fields);
                    final Transform<Struct, Type, Type> transform = new Transform<>(
                            parameters,
                            outputType,
                            inputFieldSchemas,
                            t -> t,
                            StructSchemaUtil::getAsString,
                            StructSchemaUtil::merge,
                            StructSchemaUtil::create);
                    final PCollection<Struct> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(SerializableCoder.of(Struct.class));
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, outputType));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Schema.Builder builder = RowSchemaUtil.toBuilder(input.getSchema());
                    final Map<String, Schema> inputFieldSchemas = new HashMap<>();
                    for(final TokenizeTransformParameters.TokenizeParameter field : parameters.getFields()) {
                        final Schema fieldSchema = createOutputSchema(field);
                        builder.addField(field.getName(), Schema.FieldType.array(Schema.FieldType.row(fieldSchema)).withNullable(true));
                        inputFieldSchemas.put(field.getName(), fieldSchema);
                    }
                    final Schema outputSchema = builder.build();
                    final Transform<Entity, Schema, Schema> transform = new Transform<>(
                            parameters,
                            outputSchema,
                            inputFieldSchemas,
                            s -> s,
                            EntitySchemaUtil::getAsString,
                            EntitySchemaUtil::merge,
                            EntitySchemaUtil::create);
                    final PCollection<Entity> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(SerializableCoder.of(Entity.class));
                    results.put(name, FCollection.of(name, output, DataType.ENTITY, outputSchema));
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }

        return results;
    }

    public static class Transform<T, InputSchemaT, RuntimeSchemaT> extends PTransform<PCollection<T>, PCollection<T>> {

        private final TokenizeTransformParameters parameters;
        private final InputSchemaT inputSchema;
        private final Map<String, InputSchemaT> inputFieldSchemas;
        private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;

        private final StringGetter<T> stringGetter;
        private final ValuesSetter<T, RuntimeSchemaT> valuesSetter;
        private final ValueCreator<T, RuntimeSchemaT> valueCreator;

        private Transform(final TokenizeTransformParameters parameters,
                          final InputSchemaT inputSchema,
                          final Map<String, InputSchemaT> inputFieldSchemas,
                          final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                          final StringGetter<T> stringGetter,
                          final ValuesSetter<T, RuntimeSchemaT> valuesSetter,
                          final ValueCreator<T, RuntimeSchemaT> valueCreator) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.inputFieldSchemas = inputFieldSchemas;
            this.schemaConverter = schemaConverter;
            this.stringGetter = stringGetter;
            this.valuesSetter = valuesSetter;
            this.valueCreator = valueCreator;
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            return input
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("Tokenize", ParDo.of(new TokenizeDoFn(
                            parameters.getFields(), stringGetter, valuesSetter, valueCreator, schemaConverter,
                            inputSchema, inputFieldSchemas)));
        }


        private class TokenizeDoFn extends DoFn<T, T> {

            private final List<TokenizeTransformParameters.TokenizeParameter> fields;
            private final StringGetter<T> stringGetter;
            private final ValuesSetter<T, RuntimeSchemaT> valuesSetter;
            private final ValueCreator<T, RuntimeSchemaT> valueCreator;
            private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final InputSchemaT inputSchema;
            private final Map<String, InputSchemaT> inputFieldSchemas;

            private transient RuntimeSchemaT runtimeSchema;
            private transient Map<String, RuntimeSchemaT> runtimeFieldSchemas;
            private transient Map<String, Analyzer> analyzers;

            public TokenizeDoFn(final List<TokenizeTransformParameters.TokenizeParameter> fields,
                                final StringGetter<T> stringGetter,
                                final ValuesSetter<T, RuntimeSchemaT> valuesSetter,
                                final ValueCreator<T, RuntimeSchemaT> valueCreator,
                                final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                final InputSchemaT inputSchema,
                                final Map<String, InputSchemaT> inputFieldSchemas) {

                this.fields = fields;
                this.stringGetter = stringGetter;
                this.valuesSetter = valuesSetter;
                this.valueCreator = valueCreator;
                this.schemaConverter = schemaConverter;
                this.inputSchema = inputSchema;
                this.inputFieldSchemas = inputFieldSchemas;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = this.schemaConverter.convert(inputSchema);
                this.runtimeFieldSchemas = inputFieldSchemas.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey(), e -> schemaConverter.convert(e.getValue())));
                this.analyzers = new HashMap<>();
                for(final TokenizeTransformParameters.TokenizeParameter field : this.fields) {
                    LOG.info("Add analyzer for field: " + field.getName());
                    this.analyzers.put(field.getName(), new TokenAnalyzer(field.getCharFilters(), field.getTokenizer(), field.getFilters()));
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final T element = c.element();
                final Map<String, Object> results = new HashMap<>();
                for(final TokenizeTransformParameters.TokenizeParameter field : fields) {
                    final String text = stringGetter.getAsString(element, field.getInput());
                    if(text != null) {
                        final List<T> tokens = new ArrayList<>();
                        final Analyzer analyzer = analyzers.get(field.getName());
                        final List<Map<String, Object>> tokenMaps = extractTokens(analyzer, field.getName(), text, field.getTokenizer().getType());
                        for(final Map<String, Object> tokenMap : tokenMaps) {
                            final T token = valueCreator.create(runtimeFieldSchemas.get(field.getName()), tokenMap);
                            tokens.add(token);
                        }
                        results.put(field.getName(), tokens);
                    } else {
                        results.put(field.getName(), new ArrayList<>());
                    }
                }
                final T output = valuesSetter.setValues(runtimeSchema, element, results);
                c.output(output);
            }

            private List<Map<String, Object>> extractTokens(final Analyzer analyzer,
                                                            final String field,
                                                            final String text,
                                                            final TokenAnalyzer.TokenizerType type) {

                final List<Map<String, Object>> tokens = new ArrayList<>();
                try(final TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(text))) {
                    tokenStream.reset();
                    while(tokenStream.incrementToken()) {
                        final Map<String, Object> token = new HashMap<>();
                        final CharTermAttribute cta = tokenStream.getAttribute(CharTermAttribute.class);
                        final OffsetAttribute oa = tokenStream.getAttribute(OffsetAttribute.class);
                        final TypeAttribute ta = tokenStream.getAttribute(TypeAttribute.class);
                        token.put("token", cta.toString());
                        token.put("startOffset", oa.startOffset());
                        token.put("endOffset", oa.endOffset());
                        token.put("type", ta.type());

                        if(TokenAnalyzer.TokenizerType.JapaneseTokenizer.equals(type)) {
                            final PartOfSpeechAttribute psa = tokenStream.getAttribute(PartOfSpeechAttribute.class);
                            final InflectionAttribute ia = tokenStream.getAttribute(InflectionAttribute.class);
                            final BaseFormAttribute bfa = tokenStream.getAttribute(BaseFormAttribute.class);
                            final ReadingAttribute ra = tokenStream.getAttribute(ReadingAttribute.class);
                            token.put("partOfSpeech", psa.getPartOfSpeech());
                            token.put("inflectionForm", ia.getInflectionForm());
                            token.put("inflectionType", ia.getInflectionType());
                            token.put("baseForm", bfa.getBaseForm());
                            token.put("pronunciation", ra.getPronunciation());
                            token.put("reading", ra.getReading());
                        }

                        tokens.add(token);
                    }
                    tokenStream.end();
                    return tokens;
                } catch (IOException e) {
                    return tokens;
                }
            }

        }

    }

    //
    private static Schema createOutputSchema(final TokenizeTransformParameters.TokenizeParameter parameter) {
        final Schema.Builder builder = Schema.builder();
        builder.addField("token", Schema.FieldType.STRING);
        builder.addField("startOffset", Schema.FieldType.INT32);
        builder.addField("endOffset", Schema.FieldType.INT32);
        builder.addField("type", Schema.FieldType.STRING.withNullable(true));

        if(TokenAnalyzer.TokenizerType.JapaneseTokenizer.equals(parameter.getTokenizer().getType())) {
            builder.addField("partOfSpeech", Schema.FieldType.STRING.withNullable(true));
            builder.addField("inflectionForm", Schema.FieldType.STRING.withNullable(true));
            builder.addField("inflectionType", Schema.FieldType.STRING.withNullable(true));
            builder.addField("baseForm", Schema.FieldType.STRING.withNullable(true));
            builder.addField("pronunciation", Schema.FieldType.STRING.withNullable(true));
            builder.addField("reading", Schema.FieldType.STRING.withNullable(true));
        }

        return builder.build();
    }

    private static org.apache.avro.Schema createOutputAvroSchema(final TokenizeTransformParameters.TokenizeParameter parameter) {
        final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = SchemaBuilder.record(parameter.getName())
                .fields()
                .name("token").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("startOffset").type(AvroSchemaUtil.REQUIRED_INT).noDefault()
                .name("endOffset").type(AvroSchemaUtil.REQUIRED_INT).noDefault()
                .name("type").type(AvroSchemaUtil.NULLABLE_STRING).noDefault();

        if(TokenAnalyzer.TokenizerType.JapaneseTokenizer.equals(parameter.getTokenizer().getType())) {
            return builder
                    .name("partOfSpeech").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name("inflectionForm").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name("inflectionType").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name("baseForm").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name("pronunciation").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .name("reading").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                    .endRecord();
        }

        return builder.endRecord();
    }

    private static Type createOutputType(final TokenizeTransformParameters.TokenizeParameter parameter) {

        final List<Type.StructField> fields = new ArrayList<>();
        fields.add(Type.StructField.of("token", Type.string()));
        fields.add(Type.StructField.of("startOffset", Type.int64()));
        fields.add(Type.StructField.of("endOffset", Type.int64()));
        fields.add(Type.StructField.of("type", Type.string()));

        if(TokenAnalyzer.TokenizerType.JapaneseTokenizer.equals(parameter.getTokenizer().getType())) {
            fields.add(Type.StructField.of("partOfSpeech", Type.string()));
            fields.add(Type.StructField.of("inflectionForm", Type.string()));
            fields.add(Type.StructField.of("inflectionType", Type.string()));
            fields.add(Type.StructField.of("baseForm", Type.string()));
            fields.add(Type.StructField.of("pronunciation", Type.string()));
            fields.add(Type.StructField.of("reading", Type.string()));
        }

        return Type.struct(fields);
    }


    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(InputSchemaT schema);
    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(final T value, final String field);
    }

    private interface ValuesSetter<T, SchemaT> extends Serializable {
        T setValues(final SchemaT schema, final T element, final Map<String, ? extends Object> values);
    }

    private interface ValueCreator<T, SchemaT> extends Serializable {
        T create(final SchemaT schema, final Map<String, Object> values);
    }

}
