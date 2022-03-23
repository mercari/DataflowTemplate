package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.SolrSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.FixedFileNaming;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.converter.EntityToSolrDocumentConverter;
import com.mercari.solution.util.converter.RecordToSolrDocumentConverter;
import com.mercari.solution.util.converter.RowToSolrDocumentConverter;
import com.mercari.solution.util.converter.StructToSolrDocumentConverter;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


public class SolrIndexSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexSink.class);

    private class SolrIndexSinkParameters implements Serializable {

        private String output;
        private String dynamicSplitField;
        private String tempDirectory;
        private String confDirectoryOutput;

        private String datetimeFormat;
        private String datetimeFormatZone;
        private Boolean useOnlyEndDatetime;

        private String coreName;
        private JsonElement indexSchema;

        private List<DictionaryParameters> dictionaries;
        private List<SynonymParameters> synonyms;
        private List<StopWordsParameters> stopwords;

        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public String getDynamicSplitField() {
            return dynamicSplitField;
        }

        public void setDynamicSplitField(String dynamicSplitField) {
            this.dynamicSplitField = dynamicSplitField;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void setTempDirectory(String tempDirectory) {
            this.tempDirectory = tempDirectory;
        }

        public String getConfDirectoryOutput() {
            return confDirectoryOutput;
        }

        public void setConfDirectoryOutput(String confDirectoryOutput) {
            this.confDirectoryOutput = confDirectoryOutput;
        }

        public String getDatetimeFormat() {
            return datetimeFormat;
        }

        public void setDatetimeFormat(String datetimeFormat) {
            this.datetimeFormat = datetimeFormat;
        }

        public String getDatetimeFormatZone() {
            return datetimeFormatZone;
        }

        public void setDatetimeFormatZone(String datetimeFormatZone) {
            this.datetimeFormatZone = datetimeFormatZone;
        }

        public Boolean getUseOnlyEndDatetime() {
            return useOnlyEndDatetime;
        }

        public void setUseOnlyEndDatetime(Boolean useOnlyEndDatetime) {
            this.useOnlyEndDatetime = useOnlyEndDatetime;
        }

        public String getCoreName() {
            return coreName;
        }

        public void setCoreName(String coreName) {
            this.coreName = coreName;
        }

        public JsonElement getIndexSchema() {
            return indexSchema;
        }

        public void setIndexSchema(JsonElement indexSchema) {
            this.indexSchema = indexSchema;
        }

        public List<DictionaryParameters> getDictionaries() {
            return dictionaries;
        }

        public void setDictionaries(List<DictionaryParameters> dictionaries) {
            this.dictionaries = dictionaries;
        }

        public List<SynonymParameters> getSynonyms() {
            return synonyms;
        }

        public void setSynonyms(List<SynonymParameters> synonyms) {
            this.synonyms = synonyms;
        }

        public List<StopWordsParameters> getStopwords() {
            return stopwords;
        }

        public void setStopwords(List<StopWordsParameters> stopwords) {
            this.stopwords = stopwords;
        }

    }

    private class FileParameters implements Serializable {

        private String inputName;
        private String inputPath;
        private String filename;
        private String field;
        private FieldType type;

        public String getInputName() {
            return inputName;
        }

        public void setInputName(String inputName) {
            this.inputName = inputName;
        }

        public String getInputPath() {
            return inputPath;
        }

        public void setInputPath(String inputPath) {
            this.inputPath = inputPath;
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public FieldType getType() {
            return type;
        }

        public void setType(FieldType type) {
            this.type = type;
        }

        public boolean useInput() {
            if(inputPath != null) {
                return false;
            } else {
                return inputName != null;
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

        public void setDefault() {
            if(this.type == null) {
                this.type = FieldType.direct;
            }
        }

    }

    private class DictionaryParameters extends FileParameters implements Serializable {

        private String textField;
        private String partOfSpeechTag;
        private String partOfSpeechTagField;
        private List<String> tokenFields;
        private List<String> readingFields;

        public String getTextField() {
            return textField;
        }

        public void setTextField(String textField) {
            this.textField = textField;
        }

        public String getPartOfSpeechTag() {
            return partOfSpeechTag;
        }

        public void setPartOfSpeechTag(String partOfSpeechTag) {
            this.partOfSpeechTag = partOfSpeechTag;
        }

        public String getPartOfSpeechTagField() {
            return partOfSpeechTagField;
        }

        public void setPartOfSpeechTagField(String partOfSpeechTagField) {
            this.partOfSpeechTagField = partOfSpeechTagField;
        }

        public List<String> getTokenFields() {
            return tokenFields;
        }

        public void setTokenFields(List<String> tokenFields) {
            this.tokenFields = tokenFields;
        }

        public List<String> getReadingFields() {
            return readingFields;
        }

        public void setReadingFields(List<String> readingFields) {
            this.readingFields = readingFields;
        }

        public void setDefaults() {
            super.setDefault();

            if(this.tokenFields == null) {
                this.tokenFields = new ArrayList<>();
            }
            if(this.tokenFields.size() == 0) {
                this.tokenFields.add(this.textField);
            }
            if(this.readingFields == null) {
                this.readingFields = new ArrayList<>();
            }
            if(this.readingFields.size() == 0) {
                this.readingFields.add("");
            }

            if(this.readingFields.size() > this.tokenFields.size()) {
                for(int i=0; i<this.readingFields.size() - this.tokenFields.size(); i++) {
                    this.readingFields.add("");
                }
            } else if(this.readingFields.size() < this.tokenFields.size()) {
                this.readingFields = this.readingFields.subList(0, this.tokenFields.size() - 1);
            }
        }

        public List<String> validate() {
            final List<String> errorMessages = super.validate();

            return errorMessages;
        }
    }

    private class SynonymParameters extends FileParameters implements Serializable {

        private SynonymExpandType expandType;

        public SynonymExpandType getExpandType() {
            return expandType;
        }

        public void setExpandType(SynonymExpandType expandType) {
            this.expandType = expandType;
        }

        public void setDefaults() {
            super.setDefault();
        }
    }

    private class StopWordsParameters extends FileParameters implements Serializable {

        private List<String> fields;

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public void setDefaults() {
            super.setDefault();
        }
    }

    private enum FieldType implements Serializable {
        direct,
        construct
    }

    private enum SynonymExpandType implements Serializable {
        unidirectional,
        bidirectional
    }


    public String getName() { return "solrindex"; }

    private static void validateParameters(final SolrIndexSinkParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BigtableSink parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getConfDirectoryOutput() != null) {
            if(!parameters.getConfDirectoryOutput().startsWith("gs://")) {
                errorMessages.add("SolrIndex sink module confDirectoryOutput parameter must starts with `gs://`");
            }
        } else if((parameters.getDictionaries() != null && parameters.getDictionaries().size() > 0)
                    || (parameters.getSynonyms() != null && parameters.getSynonyms().size() > 0)
                    || (parameters.getStopwords() != null && parameters.getStopwords().size() > 0)) {

            errorMessages.add("SolrIndex sink module requires confDirectoryOutput parameter when using `dictionaries`, `synonyms` or `stopwords`");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
        }

    }

    private static void setDefaultParameters(final SolrIndexSinkParameters parameters) {
        if(parameters.getConfDirectoryOutput() != null) {
            if(!parameters.getConfDirectoryOutput().endsWith("/")) {
                parameters.setConfDirectoryOutput(parameters.getConfDirectoryOutput() + "/");
            }
        }

        if(parameters.getDictionaries() == null) {
            parameters.setDictionaries(new ArrayList<>());
        } else {
            for(final DictionaryParameters dictionaryParameter : parameters.dictionaries) {
                dictionaryParameter.setDefaults();
            }
        }
        if(parameters.getSynonyms() == null) {
            parameters.setSynonyms(new ArrayList<>());
        } else {
            for(final SynonymParameters synonymParameter : parameters.synonyms) {
                synonymParameter.setDefaults();
            }
        }
        if(parameters.getStopwords() == null) {
            parameters.setStopwords(new ArrayList<>());
        } else {
            for(final StopWordsParameters stopwordsParameter : parameters.stopwords) {
                stopwordsParameter.setDefaults();
            }
        }

        if(parameters.getDatetimeFormat() == null) {
            parameters.setDatetimeFormat("yyyyMMddHHmmss");
        }
        if(parameters.getDatetimeFormatZone() == null) {
            parameters.setDatetimeFormatZone("Etc/GMT");
        }
        if(parameters.getUseOnlyEndDatetime() == null) {
            parameters.setUseOnlyEndDatetime(false);
        }
    }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits, List<FCollection<?>> sideInputs) {
        return Collections.singletonMap(config.getName(), SolrIndexSink.write(input, config, waits, sideInputs));
    }

    public static FCollection<?> write(
            final FCollection<?> collection,
            final SinkConfig config,
            final List<FCollection<?>> waits,
            final List<FCollection<?>> sideInputs) {

        final SolrIndexSinkParameters parameters = new Gson().fromJson(config.getParameters(), SolrIndexSinkParameters.class);

        validateParameters(parameters);
        setDefaultParameters(parameters);

        PCollectionTuple inputs = PCollectionTuple.of(collection.getName(), collection.getCollection());
        final Map<String,DataType> dataTypes = new HashMap<>();
        for(final FCollection<?> sideInput : sideInputs) {
            inputs = inputs.and(sideInput.getName(), sideInput.getCollection());
            dataTypes.put(sideInput.getName(), sideInput.getDataType());
        }

        final PCollection output = inputs.apply(config.getName(), new SolrWrite(collection, parameters, waits, dataTypes));
        final FCollection<?> fcollection = FCollection.update(collection, output);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return fcollection;
    }

    public static class SolrWrite extends PTransform<PCollectionTuple, PCollection<KV>> {

        private FCollection<?> collection;
        private final SolrIndexSinkParameters parameters;
        private final List<FCollection<?>> waits;
        private final Map<String,DataType> dataTypes;

        private final List<DictionaryParameters> dictionaryParameters;
        private final List<SynonymParameters> synonymParameters;
        private final List<StopWordsParameters> stopWordsParameters;

        private final String schemaString;

        private SolrWrite(final FCollection<?> collection,
                          final SolrIndexSinkParameters parameters,
                          final List<FCollection<?>> waits,
                          final Map<String,DataType> dataTypes) {

            this.collection = collection;
            this.parameters = parameters;

            this.dictionaryParameters = parameters.getDictionaries();
            this.synonymParameters = parameters.getSynonyms();
            this.stopWordsParameters = parameters.getStopwords();

            this.waits = waits;
            this.dataTypes = dataTypes;

            if (parameters.getIndexSchema() == null) {
                schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
            } else if (parameters.getIndexSchema().isJsonPrimitive() && parameters.getIndexSchema().getAsJsonPrimitive().isString()) {
                schemaString = StorageUtil.readString(parameters.getIndexSchema().getAsString());
            } else if (parameters.getIndexSchema().isJsonObject()) {
                schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
            } else {
                schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
            }

            if(parameters.getConfDirectoryOutput() != null) {
                final String schemaOutput = parameters.getConfDirectoryOutput() + "schema.xml";
                try {
                    StorageUtil.writeString(schemaOutput, schemaString);
                } catch (IOException e) {
                    throw new IllegalArgumentException("SolrIndex schema output path: " + schemaOutput + " is illegal.", e);
                }
            }
        }

        public PCollection<KV> expand(final PCollectionTuple inputs) {

            final PCollection<?> input;
            final List<KV<String,String>> dictionaryPaths = new ArrayList<>();
            final List<KV<String,String>> synonymPaths = new ArrayList<>();
            final List<KV<String,String>> stopwordsPaths = new ArrayList<>();
            if((waits == null || waits.size() == 0) && dictionaryParameters.size() == 0 && synonymParameters.size() == 0 && stopWordsParameters.size() == 0) {
                input = inputs.get(collection.getName());
            } else {
                final List<PCollection<?>> wait = new ArrayList<>();
                if(waits != null && waits.size() > 0) {
                    wait.addAll(waits.stream()
                            .map(FCollection::getCollection)
                            .collect(Collectors.toList()));
                }
                if(dictionaryParameters.size() > 0) {
                    for(final DictionaryParameters dictionaryParameter : dictionaryParameters) {
                        if(dictionaryParameter.useInput()) {
                            final String name = "CreateDictionaryFrom" + dictionaryParameter.getInputName();
                            final PCollection<?> sideInput = inputs.get(dictionaryParameter.getInputName());
                            final DataType sideInputDataType = dataTypes.get(dictionaryParameter.getInputName());
                            final PCollection<KV<String,String>> dictionaryFinished;
                            if(FieldType.direct.equals(dictionaryParameter.getType())) {
                                final PTransform<PCollection<?>,PCollection<KV<String,String>>> lineWrite = LineWrite
                                        .of(parameters.getConfDirectoryOutput(), dictionaryParameter, sideInputDataType);
                                dictionaryFinished = sideInput.apply(name, lineWrite);
                            } else {
                                final PTransform<PCollection<?>,PCollection<KV<String,String>>> dictionaryWrite = DictionaryWrite
                                        .of(parameters.getConfDirectoryOutput(), dictionaryParameter, sideInputDataType);
                                dictionaryFinished = sideInput.apply(name, dictionaryWrite);
                            }
                            wait.add(dictionaryFinished);
                            final String outputPath = parameters.getConfDirectoryOutput() + dictionaryParameter.getFilename();
                            dictionaryPaths.add(KV.of(dictionaryParameter.getFilename(), outputPath));
                        } else {
                            dictionaryPaths.add(KV.of(dictionaryParameter.getFilename(), dictionaryParameter.getInputPath()));
                        }
                    }
                }
                if(synonymParameters.size() > 0) {
                    for(final SynonymParameters synonymParameter : synonymParameters) {
                        if(synonymParameter.useInput()) {
                            final String name = "CreateSynonymFrom" + synonymParameter.getInputName();
                            final PCollection<?> sideInput = inputs.get(synonymParameter.getInputName());
                            final DataType sideInputDataType = dataTypes.get(synonymParameter.getInputName());
                            final PCollection<KV<String,String>> synonymFinished;
                            if(FieldType.direct.equals(synonymParameter.getType())) {
                                final PTransform<PCollection<?>,PCollection<KV<String,String>>> lineWrite = LineWrite
                                        .of(parameters.getConfDirectoryOutput(), synonymParameter, sideInputDataType);
                                synonymFinished = sideInput.apply(name, lineWrite);
                            } else {
                                final PTransform<PCollection<?>, PCollection<KV<String,String>>> synonymWrite = SynonymWrite
                                        .of(parameters.getConfDirectoryOutput(), synonymParameter, sideInputDataType);
                                synonymFinished = sideInput.apply(name, synonymWrite);
                            }
                            wait.add(synonymFinished);
                            final String outputPath = parameters.getConfDirectoryOutput() + synonymParameter.getFilename();
                            synonymPaths.add(KV.of(synonymParameter.getFilename(), outputPath));
                        } else {
                            synonymPaths.add(KV.of(synonymParameter.getFilename(), synonymParameter.getInputPath()));
                        }
                    }
                }
                if(stopWordsParameters.size() > 0) {
                    for(final StopWordsParameters stopwordsParameter : stopWordsParameters) {
                        if(stopwordsParameter.useInput()) {
                            final String name = "CreateStopWordsFrom" + stopwordsParameter.getInputName();
                            final PCollection<?> sideInput = inputs.get(stopwordsParameter.getInputName());
                            final DataType sideInputDataType = dataTypes.get(stopwordsParameter.getInputName());
                            final PCollection<KV<String,String>> stopwordsFinished;
                            if(FieldType.direct.equals(stopwordsParameter.getType())) {
                                final PTransform<PCollection<?>,PCollection<KV<String,String>>> lineWrite = LineWrite
                                        .of(parameters.getConfDirectoryOutput(), stopwordsParameter, sideInputDataType);
                                stopwordsFinished = sideInput.apply(name, lineWrite);
                            } else {
                                final PTransform<PCollection<?>, PCollection<KV<String,String>>> stopwordsWrite = StopWordsWrite
                                        .of(parameters.getConfDirectoryOutput(), stopwordsParameter, sideInputDataType);
                                stopwordsFinished = sideInput.apply(name, stopwordsWrite);
                            }
                            wait.add(stopwordsFinished);
                            final String outputPath = parameters.getConfDirectoryOutput() + stopwordsParameter.getFilename();
                            stopwordsPaths.add(KV.of(stopwordsParameter.getFilename(), outputPath));
                        } else {
                            stopwordsPaths.add(KV.of(stopwordsParameter.getFilename(), stopwordsParameter.getInputPath()));
                        }
                    }
                }

                input = inputs.get(collection.getName())
                        .apply("Wait", Wait.on(wait))
                        .setCoder((Coder)(inputs.get(collection.getName()).getCoder()));
            }

            final String name = "WriteIndexFile";
            final String destinationField = this.parameters.getDynamicSplitField();
            WriteFilesResult writeResult;
            switch (collection.getDataType()) {
                case AVRO: {
                    final FileIO.Write<String, GenericRecord> write = createWrite(
                            parameters, e -> AvroSchemaUtil.getAsString(e, destinationField));
                    writeResult = ((PCollection<GenericRecord>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, RecordToSolrDocumentConverter::convert,
                                            dictionaryPaths, synonymPaths, stopwordsPaths)));
                    break;
                }
                case ROW: {
                    final FileIO.Write<String, Row> write = createWrite(
                            parameters, e -> RowSchemaUtil.getAsString(e, destinationField));
                    writeResult = ((PCollection<Row>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, RowToSolrDocumentConverter::convert,
                                            dictionaryPaths, synonymPaths, stopwordsPaths)));
                    break;
                }
                case STRUCT: {
                    final FileIO.Write<String, Struct> write = createWrite(
                            parameters, e -> StructSchemaUtil.getAsString(e, destinationField));
                    writeResult = ((PCollection<Struct>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, StructToSolrDocumentConverter::convert,
                                            dictionaryPaths, synonymPaths, stopwordsPaths)));
                    break;
                }
                case ENTITY: {
                    final FileIO.Write<String, Entity> write = createWrite(
                            parameters, e -> EntitySchemaUtil.getAsString(e.getPropertiesOrDefault(destinationField, null)));
                    writeResult = ((PCollection<Entity>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, EntityToSolrDocumentConverter::convert,
                                            dictionaryPaths, synonymPaths, stopwordsPaths)));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Solr not supported input type: " + collection.getDataType());
            }

            return writeResult.getPerDestinationOutputFilenames();
        }

        private <InputT> FileIO.Write<String, InputT> createWrite(
                final SolrIndexSinkParameters parameters,
                final SerializableFunction<InputT, String> destinationFunction) {

            final String output = parameters.getOutput();

            FileIO.Write<String, InputT> write;
            if(parameters.getDynamicSplitField() != null) {
                write = FileIO.<String, InputT>writeDynamic()
                        .to(output)
                        .by(d -> Optional.ofNullable(destinationFunction.apply(d)).orElse(""))
                        .withDestinationCoder(StringUtf8Coder.of());
            } else {
                final String outdir = StorageUtil.removeDirSuffix(output);
                write = FileIO.<String, InputT>writeDynamic()
                        .to(outdir)
                        .by(d -> "")
                        .withDestinationCoder(StringUtf8Coder.of());
            }

            write = write.withNumShards(1);
            final String datetimeFormat = parameters.getDatetimeFormat();
            final String datetimeFormatZone = parameters.getDatetimeFormatZone();
            final Boolean useOnlyEndDatetime = parameters.getUseOnlyEndDatetime();
            if(parameters.getDynamicSplitField() != null) {
                write = write
                        .withNaming(key -> FixedFileNaming.of(
                                StorageUtil.addFilePrefix(output, key), "",
                                datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
            } else {
                write = write
                        .withNaming(key -> FixedFileNaming.of(
                                StorageUtil.addFilePrefix(output, ""), "",
                                datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
            }

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }

            return write;
        }

    }

    public static class LineWrite<T> extends PTransform<PCollection<T>, PCollection<KV<String,String>>> {

        private final String outputDir;
        private final FileParameters parameters;
        private final StringGetter<T> stringGetter;

        LineWrite(final String outputDir,
                  final FileParameters parameters,
                  final StringGetter<T> stringGetter) {

            this.outputDir = outputDir;
            this.parameters = parameters;
            this.stringGetter = stringGetter;
        }

        public static LineWrite of(final String outputDir, final FileParameters parameters, final DataType dataType) {
            switch (dataType) {
                case AVRO: {
                    return new LineWrite<>(outputDir, parameters, AvroSchemaUtil::getAsString);
                }
                case ROW: {
                    return new LineWrite<>(outputDir, parameters, RowSchemaUtil::getAsString);
                }
                case STRUCT: {
                    return new LineWrite<>(outputDir, parameters, StructSchemaUtil::getAsString);
                }
                case ENTITY: {
                    return new LineWrite<Entity>(outputDir, parameters, EntitySchemaUtil::getAsString);
                }
                default: {
                    throw new IllegalArgumentException("LineWrite not support dataType: " + dataType);
                }
            }
        }

        @Override
        public PCollection<KV<String,String>> expand(final PCollection<T> input) {
            final String output = outputDir + parameters.getFilename();
            final FileIO.Sink<T> sink = new LineFileSink<>(parameters, stringGetter);
            return input
                    .apply("WriteTextFile", FileIO.<String, T>writeDynamic()
                            .to(output)
                            .by(d -> "")
                            .via(sink)
                            .withNaming(key -> FixedFileNaming.of(StorageUtil.addFilePrefix(output, ""), "")))
                    .getPerDestinationOutputFilenames();
        }
    }

    public static class DictionaryWrite<T> extends PTransform<PCollection<T>, PCollection<KV<String,String>>> {

        private final String outputDir;
        private final DictionaryParameters parameters;
        private final DictionaryFormatter<T> formatter;

        DictionaryWrite(final String outputDir,
                        final DictionaryParameters parameters,
                        final DictionaryFormatter<T> formatter) {

            this.outputDir = outputDir;
            this.parameters = parameters;
            this.formatter = formatter;
        }

        public static DictionaryWrite of(final String outputDir, final DictionaryParameters parameters, final DataType dataType) {
            switch (dataType) {
                case AVRO: {
                    return new DictionaryWrite<>(outputDir, parameters,
                            (GenericRecord record, DictionaryParameters params) -> {
                                return "a";
                            });
                }
                case ROW: {
                    return new DictionaryWrite<>(outputDir, parameters,
                            (Row row, DictionaryParameters params) -> {
                                return "b";
                            });
                }
                case STRUCT: {
                    return new DictionaryWrite<>(outputDir, parameters,
                            (Struct struct, DictionaryParameters params) -> {
                                return "c";
                            });
                }
                case ENTITY: {
                    return new DictionaryWrite<>(outputDir, parameters,
                            (Entity entity, DictionaryParameters params) -> {
                                return "d";
                            });
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        }

        @Override
        public PCollection<KV<String,String>> expand(final PCollection<T> input) {
            final String output = outputDir + parameters.getFilename();
            final FileIO.Sink<T> sink = new DictionaryFileSink<>(parameters, formatter);
            return input
                    .apply("WriteDictionaryFile", FileIO.<String, T>writeDynamic()
                            .to(output)
                            .by(d -> "")
                            .via(sink)
                            .withNaming(key -> FixedFileNaming.of(StorageUtil.addFilePrefix(output, ""), "")))
                    .getPerDestinationOutputFilenames();
        }
    }

    public static class SynonymWrite<T> extends PTransform<PCollection<T>, PCollection<KV<String,String>>> {

        private final String outputDir;
        private final SynonymParameters parameters;
        private final StringGetter<T> stringGetter;
        private final SynonymFormatter<T> formatter;

        SynonymWrite(final String outputDir,
                     final SynonymParameters parameters,
                     final StringGetter<T> stringGetter,
                     final SynonymFormatter<T> formatter) {

            this.outputDir = outputDir;
            this.parameters = parameters;
            this.stringGetter = stringGetter;
            this.formatter = formatter;
        }

        public static SynonymWrite of(final String outputDir, final SynonymParameters parameters, final DataType dataType) {
            switch (dataType) {
                case AVRO: {
                    return new SynonymWrite<>(outputDir, parameters, AvroSchemaUtil::getAsString,
                            (GenericRecord record, SynonymParameters params) -> {
                                return "";
                            });
                }
                case ROW: {
                    return new SynonymWrite<>(outputDir, parameters, RowSchemaUtil::getAsString,
                            (Row row, SynonymParameters params) -> {
                                return "";
                            });
                }
                case STRUCT: {
                    return new SynonymWrite<>(outputDir, parameters, StructSchemaUtil::getAsString,
                            (Struct struct, SynonymParameters params) -> {
                                return "";
                            });
                }
                case ENTITY: {
                    return new SynonymWrite<>(outputDir, parameters, EntitySchemaUtil::getAsString,
                            (Entity entity, SynonymParameters params) -> {
                                return "";
                            });
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        }

        @Override
        public PCollection<KV<String, String>> expand(final PCollection<T> input) {
            final String output = outputDir + parameters.getFilename();
            final FileIO.Sink<T> sink = new SynonymFileSink<>(parameters, formatter);
            return input
                    .apply("WriteSynonymFile", FileIO.<String, T>writeDynamic()
                            .to(output)
                            .by(d -> "")
                            .via(sink)
                            .withNaming(key -> FixedFileNaming.of(StorageUtil.addFilePrefix(output, ""), "")))
                    .getPerDestinationOutputFilenames();
        }

    }

    public static class StopWordsWrite<T> extends PTransform<PCollection<T>, PCollection<KV<String,String>>> {

        private final String outputDir;
        private final StopWordsParameters parameters;
        private final StringGetter<T> stringGetter;

        StopWordsWrite(final String outputDir,
                       final StopWordsParameters parameters,
                       final StringGetter<T> stringGetter) {

            this.outputDir = outputDir;
            this.parameters = parameters;
            this.stringGetter = stringGetter;
        }

        public static StopWordsWrite of(final String outputDir, final StopWordsParameters parameters, final DataType dataType) {
            switch (dataType) {
                case AVRO: {
                    return new StopWordsWrite<>(outputDir, parameters, AvroSchemaUtil::getAsString);
                }
                case ROW: {
                    return new StopWordsWrite<>(outputDir, parameters, RowSchemaUtil::getAsString);
                }
                case STRUCT: {
                    return new StopWordsWrite<>(outputDir, parameters, StructSchemaUtil::getAsString);
                }
                case ENTITY: {
                    return new StopWordsWrite<Entity>(outputDir, parameters, EntitySchemaUtil::getAsString);
                }
                default: {
                    throw new IllegalArgumentException();
                }
            }
        }

        @Override
        public PCollection<KV<String, String>> expand(final PCollection<T> input) {
            final String output = outputDir + parameters.getFilename();
            final FileIO.Sink<T> sink = new StopWordsFileSink<>(parameters, stringGetter);
            return input
                    .apply("WriteSynonymFile", FileIO.<String, T>writeDynamic()
                            .to(output)
                            .by(d -> "")
                            .via(sink)
                            .withNaming(key -> FixedFileNaming.of(StorageUtil.addFilePrefix(output, ""), "")))
                    .getPerDestinationOutputFilenames();
        }

    }

    public static class LineFileSink<ElementT> implements FileIO.Sink<ElementT> {

        private final FileParameters parameters;
        private final StringGetter<ElementT> stringGetter;

        private transient PrintWriter writer;

        public LineFileSink(final FileParameters parameters,
                            final StringGetter<ElementT> stringGetter) {

            this.parameters = parameters;
            this.stringGetter = stringGetter;
        }

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            final OutputStream os = Channels.newOutputStream(channel);
            this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
        }

        @Override
        public void write(ElementT element) {
            this.writer.println(this.stringGetter.getAsString(element, parameters.getField()));
        }

        @Override
        public void flush() {
            this.writer.close();
        }

    }

    public static class DictionaryFileSink<ElementT> implements FileIO.Sink<ElementT> {

        private final DictionaryParameters parameters;
        private final DictionaryFormatter<ElementT> formatter;

        private transient PrintWriter writer;

        public DictionaryFileSink(final DictionaryParameters parameters,
                                  final DictionaryFormatter<ElementT> formatter) {

            this.parameters = parameters;
            this.formatter = formatter;
        }

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            final OutputStream os = Channels.newOutputStream(channel);
            this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
        }

        @Override
        public void write(ElementT element) {
            this.writer.println(this.formatter.formatText(element, parameters));
        }

        @Override
        public void flush() {
            this.writer.close();
        }

    }

    public static class SynonymFileSink<ElementT> implements FileIO.Sink<ElementT> {

        private final SynonymParameters parameters;
        private final SynonymFormatter<ElementT> formatter;

        private transient PrintWriter writer;

        public SynonymFileSink(final SynonymParameters parameters,
                               final SynonymFormatter<ElementT> formatter) {

            this.parameters = parameters;
            this.formatter = formatter;
        }

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            final OutputStream os = Channels.newOutputStream(channel);
            this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
        }

        @Override
        public void write(ElementT element) {
            this.writer.println(this.formatter.formatText(element, parameters));
        }

        @Override
        public void flush() {
            this.writer.close();
        }

    }

    public static class StopWordsFileSink<ElementT> implements FileIO.Sink<ElementT> {

        private final StopWordsParameters parameters;
        private final StringGetter<ElementT> stringGetter;

        private transient PrintWriter writer;

        public StopWordsFileSink(final StopWordsParameters parameters,
                                 final StringGetter<ElementT> stringGetter) {

            this.parameters = parameters;
            this.stringGetter = stringGetter;
        }

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            final OutputStream os = Channels.newOutputStream(channel);
            this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
        }

        @Override
        public void write(ElementT element) {
            this.writer.println(this.stringGetter.getAsString(element, parameters.getField()));
        }

        @Override
        public void flush() {
            this.writer.close();
        }

    }

    public interface StringGetter<ElementT> extends Serializable {
        String getAsString(ElementT element, String field);
    }

    public interface DictionaryFormatter<ElementT> extends Serializable {
        String formatText(ElementT element, DictionaryParameters parameters);
    }

    public interface SynonymFormatter<ElementT> extends Serializable {
        String formatText(ElementT element, SynonymParameters parameters);
    }

}
