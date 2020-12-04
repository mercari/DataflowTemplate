package com.mercari.solution.module.sink.fileio;

import org.apache.beam.sdk.io.FileIO;
import static org.apache.commons.compress.utils.CharsetNames.UTF_8;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.stream.Collectors;

public class TextSink<ElementT> implements FileIO.Sink<ElementT> {

    private final List<String> fields;
    private final Boolean header;
    private final RecordFormatter<ElementT> formatter;
    private final Boolean bom;
    private transient PrintWriter writer;

    private TextSink(final List<String> fields, final Boolean header, final Boolean bom, final RecordFormatter formatter) {
        this.fields = fields;
        this.header = header;
        this.bom = bom;
        this.formatter = formatter;
    }

    public static <ElementT> TextSink<ElementT> of(final List<String> fields, final RecordFormatter<ElementT> formatter) {
        return new TextSink(fields, false, false, formatter);
    }

    public static <ElementT> TextSink<ElementT> of(final List<String> fields, final Boolean header, final RecordFormatter<ElementT> formatter) {
        return new TextSink(fields, header, false, formatter);
    }

    public static <ElementT> TextSink<ElementT> of(final List<String> fields, final Boolean header, final Boolean bom, final RecordFormatter<ElementT> formatter) {
        return new TextSink(fields, header, bom, formatter);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        final OutputStream os = Channels.newOutputStream(channel);
        if(this.bom) {
            os.write(0xef);
            os.write(0xbb);
            os.write(0xbf);
        }
        this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, UTF_8)));
        if(this.header) {
            this.writer.println(this.fields.stream().collect(Collectors.joining(",")));
        }
    }

    @Override
    public void write(ElementT element) {
        this.writer.println(this.formatter.formatText(element, fields));
    }

    @Override
    public void flush() {
        this.writer.close();
    }

    public interface RecordFormatter<ElementT> extends Serializable {
        String formatText(ElementT element, List<String> fields);
    }

}
