package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.util.SolrUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.io.FileIO;
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
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class SolrSink<ElementT> implements FileIO.Sink<ElementT> {

    private static final String SOLR_XML_DEFAULT = "<solr></solr>";
    private static final String SOLR_CONFIG_XML_DEFAULT = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
            "<config>\n" +
            "    <luceneMatchVersion>9.0.0</luceneMatchVersion>\n" +
            "    <dataDir>${solr.data.dir:}</dataDir>\n" +
            "    <directoryFactory name=\"DirectoryFactory\" class=\"${solr.directoryFactory:solr.NRTCachingDirectoryFactory}\"/>\n" +
            "    <codecFactory class=\"solr.SchemaCodecFactory\"/>\n" +
            "    <indexConfig>\n" +
            "        <lockType>none</lockType>\n" +
            "    </indexConfig>\n" +
            "\n" +
            "    <lib dir=\"${solr.install.dir:../../../..}/contrib/analysis-extras/lib\" regex=\".*\\.jar\" />\n" +
            "    <lib dir=\"${solr.install.dir:../../../..}/contrib/analysis-extras/lucene-libs\" regex=\".*\\.jar\" />\n" +
            "\n" +
            "    <requestHandler name=\"/select\" class=\"solr.SearchHandler\">\n" +
            "        <lst name=\"defaults\">\n" +
            "            <str name=\"echoParams\">explicit</str>\n" +
            "            <int name=\"rows\">10</int>\n" +
            "        </lst>\n" +
            "    </requestHandler>\n" +
            "\n" +
            "    <requestHandler name=\"/suggest\" class=\"solr.SearchHandler\" startup=\"lazy\">\n" +
            "        <lst name=\"defaults\">\n" +
            "            <str name=\"suggest\">true</str>\n" +
            "            <str name=\"suggest.count\">10</str>\n" +
            "        </lst>\n" +
            "        <arr name=\"components\">\n" +
            "            <str>suggest</str>\n" +
            "        </arr>\n" +
            "    </requestHandler>\n" +
            "</config>";
    private static final Logger LOG = LoggerFactory.getLogger(SolrSink.class);

    private final String coreName;
    private final String solrSchema;
    private final RecordFormatter<ElementT> formatter;

    private transient List<String> fieldNames;

    private int count = 0;
    private transient SolrIndexWriter writer;
    private transient OutputStream outputStream;
    private transient SolrCore core;

    private SolrSink(final String coreName, final String solrSchema, final RecordFormatter<ElementT> formatter) {
        this.coreName = coreName;
        this.solrSchema = solrSchema;
        this.formatter = formatter;
    }

    public static <ElementT> SolrSink<ElementT> of(final String coreName, final String solrSchema, final RecordFormatter<ElementT> formatter) {
        return new SolrSink<>(coreName, solrSchema, formatter);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {

        final Path solrPath = Paths.get("/solr");
        solrPath.toFile().mkdir();
        try (final FileWriter filewriter = new FileWriter(new File("/solr/solr.xml"))) {
            filewriter.write(SOLR_XML_DEFAULT);
        }

        final CoreContainer container = CoreContainer.createAndLoad(solrPath);
        final Path corePath = Paths.get("/solr/" + coreName);

        corePath.toFile().mkdir();
        try (final FileWriter filewriter = new FileWriter(new File("/solr/" + this.coreName + "/solrconfig.xml"))) {
            filewriter.write(SOLR_CONFIG_XML_DEFAULT);
        }
        final String schemaString;
        if(this.solrSchema.startsWith("gs://")) {
            schemaString = StorageUtil.readString(this.solrSchema);
        } else {
            schemaString = this.solrSchema;
        }
        try (final FileWriter filewriter = new FileWriter(new File("/solr/" + this.coreName + "/schema.xml"))) {
            filewriter.write(schemaString);
        }
        this.fieldNames = SolrUtil.getFieldNames(schemaString);

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

        this.writer = SolrIndexWriter.create(core, "",
                this.core.getIndexDir(),
                this.core.getDirectoryFactory(), create,
                this.core.getLatestSchema(),
                this.core.getSolrConfig().indexConfig,
                this.core.getDeletionPolicy(),
                this.core.getCodec());
        this.outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(ElementT element) throws IOException {
        final SolrInputDocument solrDoc = formatter.formatRecord(element, fieldNames);
        final Document doc = DocumentBuilder.toDocument(solrDoc, this.core.getLatestSchema());
        this.writer.addDocument(doc);
        this.count += 1;
        if (this.count % 10000 == 0) {
            this.writer.flush();
            LOG.info("Commit documents: %d", this.count);
        }
    }

    @Override
    public void flush() throws IOException {
        //this.writer.commit();
        this.writer.flush();
        this.writer.forceMerge(1, true);
        this.writer.close();
        LOG.info(String.format("Write documents count: %d", this.count));

        final Path indexDirPath = Paths.get("/solr/" + this.coreName);
        LOG.info(String.format("Finished to create index at [%s]", indexDirPath.toFile().getAbsolutePath()));
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
            LOG.warn(file.getAbsolutePath() + " ");
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