package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.util.pipeline.union.UnionValue;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class UnionValueCsvSink implements FileIO.Sink<KV<String, UnionValue>> {

    private final List<String> fields;
    private final Boolean outputHeader;
    private final String headerLine;
    private final Boolean bom;

    private transient @Nullable PrintWriter writer;

    public static UnionValueCsvSink of(
            final List<String> fields,
            final Boolean outputHeader,
            final String headerLine,
            final boolean bom) {

        return new UnionValueCsvSink(fields, outputHeader, headerLine, bom);
    }

    UnionValueCsvSink(
            final List<String> fields,
            final Boolean outputHeader,
            final String headerLine,
            final boolean bom) {

        this.fields = fields;
        this.outputHeader = outputHeader;
        this.headerLine = headerLine;
        this.bom = bom;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        final OutputStream os = Channels.newOutputStream(channel);
        if(this.bom) {
            os.write(0xef);
            os.write(0xbb);
            os.write(0xbf);
        }
        if(this.outputHeader) {
            if(this.headerLine != null) {
                this.writer.println(headerLine);
            } else {
                this.writer.println(String.join(",", this.fields));
            }
        }
        this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
    }

    @Override
    public void write(KV<String, UnionValue> element) throws IOException {
        final UnionValue input = element.getValue();
        final String line = UnionValue.getAsCsvLine(input, fields);
        writer.append(line);
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}

