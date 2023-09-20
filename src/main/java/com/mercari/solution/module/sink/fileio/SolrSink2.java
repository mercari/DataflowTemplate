package com.mercari.solution.module.sink.fileio;

import com.google.api.services.storage.Storage;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.mercari.solution.module.sink.LocalSolrSink;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.domain.search.SolrUtil;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SolrSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.lucene.document.Document;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolrSink2 implements FileIO.Sink<UnionValue> {

    private static final String SOLR_HOME = "/solr/";
    private static final String SOLR_XML_DEFAULT = "<solr></solr>";

    private static final Logger LOG = LoggerFactory.getLogger(SolrSink.class);

    private final List<LocalSolrSink.Core> cores;
    private final List<String> inputNames;

    private int count = 0;
    private transient Map<String, SolrCore> solrCores;
    private transient Map<String, SolrIndexWriter> writers;
    private transient Map<String, List<String>> fields;
    private transient OutputStream outputStream;

    private SolrSink2(final List<LocalSolrSink.Core> cores, final List<String> inputNames) {
        this.cores = cores;
        this.inputNames = inputNames;
    }

    public static SolrSink2 of(
            final List<LocalSolrSink.Core> cores,
            final List<String> inputNames) {

        return new SolrSink2(cores, inputNames);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {

        final Path solrPath = Paths.get(SOLR_HOME);
        solrPath.toFile().mkdir();

        // Write solr.xml
        try (final FileWriter filewriter = new FileWriter(SOLR_HOME + "solr.xml")) {
            filewriter.write(SOLR_XML_DEFAULT);
        }

        if(solrCores == null) {
            solrCores = new HashMap<>();
        }
        if(writers == null) {
            writers = new HashMap<>();
        }
        if(fields == null) {
            fields = new HashMap<>();
        }

        final CoreContainer container = CoreContainer.createAndLoad(solrPath);
        for(LocalSolrSink.Core core : cores) {
            final Path corePath = Paths.get(SOLR_HOME + core.getName());
            final Path confPath = Paths.get(SOLR_HOME + core.getName() + "/conf");
            final Path langPath = Paths.get(SOLR_HOME + core.getName() + "/conf/lang");
            corePath.toFile().mkdir();
            confPath.toFile().mkdir();
            langPath.toFile().mkdir();

            // Write schema.xml
            try (final FileWriter filewriter = new FileWriter(SOLR_HOME + core.getName() + "/conf/schema.xml")) {
                LOG.info("core: " + core.getName() + ", schema.xml: " + core.getSchema());
                filewriter.write(core.getSchema());
            }

            // Write solrconfig.xml
            try (final FileWriter filewriter = new FileWriter(SOLR_HOME + core.getName() + "/conf/solrconfig.xml")) {
                LOG.info("core: " + core.getName() + ", solrconfig.xml: " + core.getConfig());
                filewriter.write(core.getConfig());
            }

            // Copy custom config files
            if(core.getCustomConfigFiles().size() > 0) {
                final Storage storage = StorageUtil.storage();
                for(final KV<String,String> confFilePath : core.getCustomConfigFiles()) {
                    final String fileName = confFilePath.getKey();
                    final String fileContent = StorageUtil.readString(storage, confFilePath.getValue());
                    try (final FileWriter filewriter = new FileWriter(SOLR_HOME + core.getName() + "/conf/" + fileName)) {
                        LOG.info("core: " + core.getName() + ", custom config file: " + fileContent);
                        filewriter.write(fileContent);
                    }
                }
            }

            final SolrCore solrCore;
            final boolean create;
            if (container.getAllCoreNames().contains(core.getName())) {
                solrCore = container.getCore(core.getName());
                create = false;
            } else {
                // For retrying
                solrCore = container.create(core.getName(), new HashMap<>());
                create = true;
            }
            this.solrCores.put(core.getName(), solrCore);
            final SolrIndexWriter writer = SolrUtil.createWriter(solrCore, core.getName(), create);
            this.writers.put(core.getName(), writer);

            this.fields.put(core.getName(), SolrSchemaUtil.getFieldNames(core.getSchema()));
        }

        this.outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(UnionValue element) throws IOException {
        final String input = inputNames.get(element.getIndex());
        final String coreName = cores.stream().filter(s -> s.getInput().equals(input)).map(LocalSolrSink.Core::getName).findAny().orElseThrow();
        final List<String> fieldNames = fields.get(coreName);
        final SolrInputDocument solrDoc = switch (element.getType()) {
            case AVRO -> RecordToSolrDocumentConverter.convert((GenericRecord) element.getValue(), fieldNames);
            case ROW -> RowToSolrDocumentConverter.convert((Row) element.getValue(), fieldNames);
            case STRUCT -> StructToSolrDocumentConverter.convert((Struct) element.getValue(), fieldNames);
            case DOCUMENT -> DocumentToSolrDocumentConverter.convert((com.google.firestore.v1.Document) element.getValue(), fieldNames);
            case ENTITY -> EntityToSolrDocumentConverter.convert((Entity) element.getValue(), fieldNames);
            default -> throw new RuntimeException("Not supported type: " + element.getType());
        };
        final Document doc = DocumentBuilder.toDocument(solrDoc, this.solrCores.get(coreName).getLatestSchema());
        this.writers.get(coreName).addDocument(doc);
        this.count += 1;
        if (this.count % 10000 == 0) {
            LOG.info(String.format("processed documents: %d", this.count));
        }
    }

    @Override
    public void flush() throws IOException {
        LOG.info(String.format("LocalSolr processed documents: %d", this.count));
        for(LocalSolrSink.Core core : cores) {
            final long start = Instant.now().toEpochMilli();
            this.writers.get(core.getName()).commit();
            this.writers.get(core.getName()).forceMerge(1, true);
            this.writers.get(core.getName()).close();
            final long millisec = Instant.now().toEpochMilli() - start;

            final String indexDir = SOLR_HOME + core.getName();
            final Path indexDirPath = Paths.get(indexDir);
            LOG.info(String.format("Core: %s Finished to create index at [%s], took %d ms.", core.getName(), indexDirPath.toFile().getAbsolutePath(), millisec));
        }

        ZipFileUtil.writeZipFile(outputStream, SOLR_HOME);
        LOG.info("Finished to upload documents!");
    }

}