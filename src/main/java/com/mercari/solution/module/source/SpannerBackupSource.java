package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SpannerBackupSource implements SourceModule {

    private class SpannerBackupSourceParameters implements Serializable {

        private String input;
        private List<String> tables;

        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        public List<String> getTables() {
            return tables;
        }

        public void setTables(List<String> tables) {
            this.tables = tables;
        }
    }

    public String getName() { return "spannerBackup"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if (config.getMicrobatch() != null && config.getMicrobatch()) {
            throw new IllegalArgumentException("SpannerBackupSource does not support microbatch mode.");
        }
        return new HashMap<>(SpannerBackupSource.batch(begin, config));
    }

    public static Map<String, FCollection<GenericRecord>> batch(final PBegin begin, final SourceConfig config) {
        final SpannerBackupBatchRead source = new SpannerBackupBatchRead(config);
        final PCollectionTuple tuple = begin.apply(config.getName(), source);
        final Map<String, FCollection<GenericRecord>> collections = new HashMap<>();
        for(final TupleTag tag : tuple.getAll().keySet()) {
            final String name = config.getName() + "." + tag.getId();
            final FCollection fcollection = FCollection
                    .of(name, tuple.<GenericRecord>get(tag), DataType.AVRO, source.schemas.get(tag.getId()));
            collections.put(name, fcollection);
        }
        return collections;
    }

    public static class SpannerBackupBatchRead extends PTransform<PBegin, PCollectionTuple> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerBackupBatchRead.class);

        private final SpannerBackupSourceParameters parameters;
        private final Map<String, Schema> schemas;

        private SpannerBackupBatchRead(final SourceConfig config) {
            this.parameters = new Gson().fromJson(config.getParameters(), SpannerBackupSourceParameters.class);
            this.schemas = new HashMap<>();
            validateParameters();
            setDefaultParameters();
        }

        public PCollectionTuple expand(final PBegin begin) {
            //final List<String> tables = parameters.getTables() == null ? null
            //        : Arrays.asList(parameters.getTables().split(","));
            PCollectionTuple tuple = PCollectionTuple.empty(begin.getPipeline());
            final Map<String, List<String>> exportTables = readExportFile(parameters.getInput());
            for(Map.Entry<String, List<String>> entry : exportTables.entrySet()) {
                if(entry.getValue().size() == 0) {
                    continue;
                }
                if(parameters.getTables() != null && !parameters.getTables().contains(entry.getKey())) {
                    continue;
                }
                final KV<Schema, PCollection<GenericRecord>> records
                        = readRecords(begin, entry.getKey(), entry.getValue());
                tuple = tuple.and(entry.getKey(), records.getValue());
                schemas.put(entry.getKey(), records.getKey());
            }

            return tuple;
        }

        private KV<Schema, PCollection<GenericRecord>> readRecords(
                final PBegin begin, final String tableName, final List<String> files) {

            final Schema avroSchema = StorageUtil.getAvroSchema(files.get(0));
            final PCollection<GenericRecord> records = begin
                    .apply("Files." + tableName, Create.of(files))
                    .apply("Match." + tableName, FileIO.matchAll().withConfiguration(FileIO.MatchConfiguration.create(EmptyMatchTreatment.ALLOW)))
                    .apply("Reads." + tableName, FileIO.readMatches())
                    .apply("Parse." + tableName, AvroIO.readFilesGenericRecords(avroSchema))
                    .setCoder(AvroCoder.of(avroSchema));

            return KV.of(avroSchema, records);
        }

        private void validateParameters() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getInput() == null) {
                errorMessages.add("Parameter must contain input");
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {

        }

        public Map<String, List<String>> readExportFile(final String input) {
            final String exportFilePath;
            final String exportDirPath;
            if(input.endsWith("spanner-export.json")) {
                exportFilePath = input;
            } else if(input.endsWith("/")) {
                exportFilePath = input + "spanner-export.json";
            } else {
                exportFilePath = input + "/spanner-export.json";
            }
            exportDirPath = exportFilePath.replace("spanner-export.json", "");
            final Map<String, List<String>> paths = new HashMap<>();
            LOG.info(exportFilePath);
            final String exportContent = StorageUtil.readString(exportFilePath);
            final JsonObject exportFile = new Gson().fromJson(exportContent, JsonObject.class);
            for(final JsonElement table : exportFile.getAsJsonArray("tables")) {
                final String tableName = table.getAsJsonObject().get("name").getAsString();
                final String manifestFilePath = table.getAsJsonObject().get("manifestFile").getAsString();

                final String manifestContent = StorageUtil.readString(exportDirPath + manifestFilePath);
                final JsonObject manifestFile = new Gson().fromJson(manifestContent, JsonObject.class);
                final List<String> files = new ArrayList<>();
                for(final JsonElement avroFile : manifestFile.getAsJsonArray("files")) {
                    final String avroFilePath = avroFile.getAsJsonObject().get("name").getAsString();
                    files.add(exportDirPath + avroFilePath);
                }

                paths.put(tableName, files);
            }
            return paths;
        }

    }

}