package com.mercari.solution.module.sink.fileio;

import com.google.api.services.storage.Storage;
import com.mercari.solution.util.XmlUtil;
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
import org.w3c.dom.Element;

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

    private final List<KV<String,String>> dictionaries;
    private final List<KV<String,String>> synonyms;
    private final List<KV<String,String>> stopwords;

    private transient List<String> fieldNames;

    private int count = 0;
    private transient SolrIndexWriter writer;
    private transient OutputStream outputStream;
    private transient SolrCore core;

    private SolrSink(final String coreName,
                     final String solrSchema,
                     final RecordFormatter<ElementT> formatter,
                     final List<KV<String,String>> dictionaries,
                     final List<KV<String,String>> synonyms,
                     final List<KV<String,String>> stopwords) {

        this.coreName = coreName;
        this.solrSchema = solrSchema;
        this.formatter = formatter;

        this.dictionaries = dictionaries;
        this.synonyms = synonyms;
        this.stopwords = stopwords;
    }

    public static <ElementT> SolrSink<ElementT> of(
            final String coreName,
            final String solrSchema,
            final RecordFormatter<ElementT> formatter,
            final List<KV<String,String>> dictionaries,
            final List<KV<String,String>> synonyms,
            final List<KV<String,String>> stopwords) {

        return new SolrSink<>(coreName, solrSchema, formatter, dictionaries, synonyms, stopwords);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {

        final Path solrPath = Paths.get("/solr");
        solrPath.toFile().mkdir();
        try (final FileWriter filewriter = new FileWriter("/solr/solr.xml")) {
            filewriter.write(SOLR_XML_DEFAULT);
        }

        final CoreContainer container = CoreContainer.createAndLoad(solrPath);
        final Path corePath = Paths.get("/solr/" + coreName);

        corePath.toFile().mkdir();
        try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/solrconfig.xml")) {
            final org.w3c.dom.Document doc = createSolrConfig();
            final String solrconfig = XmlUtil.toString(doc);
            filewriter.write(solrconfig);
        }
        final String schemaString;
        if(this.solrSchema.startsWith("gs://")) {
            schemaString = StorageUtil.readString(this.solrSchema);
        } else {
            schemaString = this.solrSchema;
        }
        try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/schema.xml")) {
            filewriter.write(schemaString);
        }

        if(dictionaries.size() > 0 || synonyms.size() > 0 || stopwords.size() > 0) {
            final Storage storage = StorageUtil.storage();
            for(final KV<String,String> dictionaryPath : dictionaries) {
                final String filename = dictionaryPath.getKey();
                final String dictionary = StorageUtil.readString(storage, dictionaryPath.getValue());
                try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/" + filename)) {
                    filewriter.write(dictionary);
                }
            }
            for(final KV<String,String> synonymPath : synonyms) {
                final String filename = synonymPath.getKey();
                final String synonym = StorageUtil.readString(storage, synonymPath.getValue());
                try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/" + filename)) {
                    filewriter.write(synonym);
                }
            }
            for(final KV<String,String> stopwordPath : stopwords) {
                final String filename = stopwordPath.getKey();
                final String stopword = StorageUtil.readString(storage, stopwordPath.getValue());
                try (final FileWriter filewriter = new FileWriter("/solr/" + this.coreName + "/" + filename)) {
                    filewriter.write(stopword);
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
            LOG.info(String.format("Commit documents: %d", this.count));
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
            LOG.info(file.getAbsolutePath() + " ");
            int len;
            byte[] buf = new byte[1024 * 1024];
            while ((len = is.read(buf)) != -1) {
                zos.write(buf, 0, len);
            }
        }
    }

    private org.w3c.dom.Document createSolrConfig() {
        final org.w3c.dom.Document document = XmlUtil.createDocument("config");
        final Element root = document.getDocumentElement();

        final Element luceneMatchVersion = document.createElement("luceneMatchVersion");
        luceneMatchVersion.setTextContent("9.0.0");
        root.appendChild(luceneMatchVersion);

        final Element dataDir = document.createElement("dataDir");
        dataDir.setTextContent("${solr.data.dir:}");
        root.appendChild(dataDir);

        final Element directoryFactory = document.createElement("directoryFactory");
        directoryFactory.setAttribute("name", "DirectoryFactory");
        directoryFactory.setAttribute("class", "${solr.directoryFactory:solr.NRTCachingDirectoryFactory}");
        root.appendChild(directoryFactory);

        final Element codecFactory = document.createElement("codecFactory");
        codecFactory.setAttribute("class", "solr.SchemaCodecFactory");
        root.appendChild(codecFactory);

        final Element indexConfig = document.createElement("indexConfig");
        final Element lockType = document.createElement("lockType");
        lockType.setTextContent("none");
        indexConfig.appendChild(lockType);
        root.appendChild(indexConfig);

        // libs
        final Element lib1 = document.createElement("lib");
        lib1.setAttribute("dir", "${solr.install.dir:../../../..}/contrib/analysis-extras/lib");
        lib1.setAttribute("regex", ".*\\.jar");
        root.appendChild(lib1);
        final Element lib2 = document.createElement("lib");
        lib2.setAttribute("dir", "${solr.install.dir:../../../..}/contrib/analysis-extras/lucene-libs");
        lib2.setAttribute("regex", ".*\\.jar");
        root.appendChild(lib2);

        // requestHandler select
        {
            final Element requestHandler = document.createElement("requestHandler");
            requestHandler.setAttribute("name", "/select");
            requestHandler.setAttribute("class", "solr.SearchHandler");
            final Element lst = document.createElement("lst");
            lst.setAttribute("name", "defaults");
            final Element stre = document.createElement("str");
            stre.setAttribute("name", "echoParams");
            stre.setTextContent("explicit");
            lst.appendChild(stre);
            final Element inte = document.createElement("int");
            inte.setAttribute("name", "rows");
            inte.setTextContent("10");
            lst.appendChild(inte);
            requestHandler.appendChild(lst);

            root.appendChild(requestHandler);
        }
        // requestHandler suggest
        {
            final Element requestHandler = document.createElement("requestHandler");
            requestHandler.setAttribute("name", "/suggest");
            requestHandler.setAttribute("class", "solr.SearchHandler");
            requestHandler.setAttribute("startup", "lazy");

            final Element lst1 = document.createElement("lst");
            lst1.setAttribute("name", "defaults");
            final Element str1 = document.createElement("str");
            str1.setAttribute("name", "suggest");
            str1.setTextContent("true");
            lst1.appendChild(str1);
            final Element str2 = document.createElement("str");
            str2.setAttribute("name", "suggest.count");
            str2.setTextContent("10");
            lst1.appendChild(str2);
            requestHandler.appendChild(lst1);

            final Element lst2 = document.createElement("arr");
            lst2.setAttribute("name", "components");
            final Element str3 = document.createElement("str");
            str3.setTextContent("suggest");
            lst2.appendChild(str3);
            requestHandler.appendChild(lst2);

            root.appendChild(requestHandler);
        }

        return document;
    }

    public interface RecordFormatter<ElementT> extends Serializable {
        SolrInputDocument formatRecord(ElementT element, List<String> fieldNames);
    }

}