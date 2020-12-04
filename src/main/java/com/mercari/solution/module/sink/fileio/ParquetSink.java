package com.mercari.solution.module.sink.fileio;


import org.apache.avro.Schema;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ParquetSink<ElementT> implements FileIO.Sink<ElementT> {

    private final String jsonSchema;
    private final GenericRecordFormatter<ElementT> formatter;
    private transient Schema schema;
    private transient ParquetWriter<GenericRecord> writer;

    private ParquetSink(final String jsonSchema, final GenericRecordFormatter formatter) {
        this.jsonSchema = jsonSchema;
        this.formatter = formatter;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.schema = new Schema.Parser().parse(this.jsonSchema);
        final BeamParquetOutputFile beamParquetOutputFile =
                new BeamParquetOutputFile(Channels.newOutputStream(channel));
        this.writer = AvroParquetWriter.<GenericRecord>builder(beamParquetOutputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(OVERWRITE).enableValidation()
                .build();
    }

    @Override
    public void write(ElementT element) throws IOException {
        this.writer.write(this.formatter.formatRecord(schema, element));
    }

    @Override
    public void flush() throws IOException {
        this.writer.close();
    }

    public static <ElementT> ParquetSink<ElementT> of(
            final Schema schema,
            final GenericRecordFormatter<ElementT> formatter) {

        return new ParquetSink(schema.toString(), formatter);
    }

    private static class BeamParquetOutputFile implements OutputFile {

        private final OutputStream outputStream;

        BeamParquetOutputFile(final OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new BeamOutputStream(outputStream);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new BeamOutputStream(outputStream);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static class BeamOutputStream extends PositionOutputStream {
        private long position = 0;
        private final OutputStream outputStream;

        private BeamOutputStream(final OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            position++;
            outputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }

}
