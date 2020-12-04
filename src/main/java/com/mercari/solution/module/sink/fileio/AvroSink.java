package com.mercari.solution.module.sink.fileio;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class AvroSink<ElementT> implements FileIO.Sink<ElementT>  {

    private final String jsonSchema;
    private final GenericRecordFormatter<ElementT> formatter;
    private transient Schema schema;
    private transient DataFileWriter<GenericRecord> writer;

    private AvroSink(final String jsonSchema, final GenericRecordFormatter<ElementT> formatter) {
        this.jsonSchema = jsonSchema;
        this.formatter = formatter;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.schema = new Schema.Parser().parse(this.jsonSchema);
        if(this.schema == null) {
            throw new IllegalArgumentException(String.format("Illegal avro schema: %s", this.jsonSchema));
        }
        this.writer = new DataFileWriter<>(new GenericDatumWriter<>(this.schema));
        this.writer.setCodec(CodecFactory.snappyCodec());
        this.writer.create(this.schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(ElementT element) throws IOException {
        this.writer.append(this.formatter.formatRecord(schema, element));
    }

    @Override
    public void flush() throws IOException {
        this.writer.flush();
    }

    public static <ElementT> AvroSink<ElementT> of(
            final String schemaJson,
            final GenericRecordFormatter<ElementT> formatter) {

        return new AvroSink<>(schemaJson, formatter);
    }

    public static <ElementT> AvroSink<ElementT> of(
            final Schema schema,
            final GenericRecordFormatter<ElementT> formatter) {

        return new AvroSink<>(schema.toString(), formatter);
    }

}
