package com.mercari.solution.module.sink.fileio;

import com.google.api.services.storage.Storage;
import com.mercari.solution.util.domain.search.SolrUtil;
import com.mercari.solution.util.schema.SolrSchemaUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.apache.lucene.document.Document;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class SolrSink<ElementT> implements FileIO.Sink<ElementT> {

    private static final String SOLR_XML_DEFAULT = "<solr></solr>";

    private static final Logger LOG = LoggerFactory.getLogger(SolrSink.class);

    private final String coreName;
    private final String solrSchema;
    private final String solrConfigXml;
    private final RecordFormatter<ElementT> formatter;
    private final List<KV<String,String>> customConfFiles;

    private transient List<String> fieldNames;

    private int count = 0;
    private transient SolrIndexWriter writer;
    private transient OutputStream outputStream;
    private transient SolrCore core;

    private SolrSink(final String coreName,
                     final String solrSchema,
                     final String solrConfigXml,
                     final List<KV<String, String>> customConfFiles,
                     final RecordFormatter<ElementT> formatter) {

        this.coreName = coreName;
        this.solrSchema = solrSchema;
        this.solrConfigXml = solrConfigXml;
        this.customConfFiles = customConfFiles;

        this.formatter = formatter;
    }

    public static <ElementT> SolrSink<ElementT> of(
            final String coreName,
            final String solrSchema,
            final String solrConfigXml,
            final List<KV<String, String>> customConfFiles,
            final RecordFormatter<ElementT> formatter) {

        return new SolrSink<>(coreName, solrSchema, solrConfigXml, customConfFiles, formatter);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {

        final Path solrPath = Paths.get("/solr");
        solrPath.toFile().mkdir();

        // Write solr.xml
        try (final FileWriter filewriter = new FileWriter("/solr/solr.xml")) {
            filewriter.write(SOLR_XML_DEFAULT);
        }

        final CoreContainer container = CoreContainer.createAndLoad(solrPath);
        final Path corePath = Paths.get("/solr/" + coreName);
        final Path confPath = Paths.get("/solr/" + coreName + "/conf");
        corePath.toFile().mkdir();
        confPath.toFile().mkdir();

        // Write solrconfig.xml
        try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/conf/solrconfig.xml")) {
            filewriter.write(solrConfigXml);
        }

        // Write schema.xml
        final String schemaString;
        if(this.solrSchema.startsWith("gs://")) {
            schemaString = StorageUtil.readString(this.solrSchema);
        } else {
            schemaString = this.solrSchema;
        }
        try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/conf/schema.xml")) {
            filewriter.write(schemaString);
        }

        // Copy files in conf/
        if(customConfFiles.size() > 0) {
            final Storage storage = StorageUtil.storage();
            for(final KV<String,String> confFilePath : customConfFiles) {
                final String fileName = confFilePath.getKey();
                final String fileContent = StorageUtil.readString(storage, confFilePath.getValue());
                try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/conf/" + fileName)) {
                    filewriter.write(fileContent);
                }
            }
        }

        this.fieldNames = SolrSchemaUtil.getFieldNames(schemaString);

        final boolean create;
        if (container.getAllCoreNames().size() > 0) {
            // Index dir must be one you specified.
            this.core = container.getCores().iterator().next();
            create = false;
        } else {
            // For retrying
            this.core = container.create(this.coreName, new HashMap<>());
            create = true;
        }

        this.writer = SolrUtil.createWriter(core, "Shops", create);
        this.outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(ElementT element) throws IOException {
        final SolrInputDocument solrDoc = formatter.formatRecord(element, fieldNames);
        final Document doc = DocumentBuilder.toDocument(solrDoc, this.core.getLatestSchema());
        this.writer.addDocument(doc);
        this.count += 1;
        if (this.count % 10000 == 0) {
            LOG.info(String.format("Core: %s processed documents: %d", this.coreName, this.count));
        }
    }

    @Override
    public void flush() throws IOException {
        LOG.info(String.format("Core: %s processed documents: %d", this.coreName, this.count));
        final long start = Instant.now().toEpochMilli();
        this.writer.commit();
        this.writer.forceMerge(1, true);
        this.writer.close();

        final Path indexDirPath = Paths.get("/solr/" + this.coreName);
        final long millisec = Instant.now().toEpochMilli() - start;
        LOG.info(String.format("Core: %s Finished to create index at [%s], took %d ms.", this.coreName, indexDirPath.toFile().getAbsolutePath(), millisec));
        try (final ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(outputStream))) {
            writeFile(zos, indexDirPath.toFile());
            zos.flush();
        }
        LOG.info("Finished to upload documents!");
    }

    private void writeFile(final ZipOutputStream zos, final File file) throws IOException {
        if (file.isDirectory()) {
            for (final File childFile : file.listFiles()) {
                writeFile(zos, childFile);
            }
            return;
        }
        zos.putNextEntry(new ZipEntry(file.getAbsolutePath().replaceFirst("/solr/", "")));
        try (final InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            LOG.info(file.getAbsolutePath() + " ");
            int len;
            byte[] buf = new byte[1024 * 1024];
            while ((len = is.read(buf)) != -1) {
                zos.write(buf, 0, len);
            }
        }
    }

    public interface RecordFormatter<ElementT> extends Serializable {
        SolrInputDocument formatRecord(ElementT element, List<String> fieldNames);
    }

}