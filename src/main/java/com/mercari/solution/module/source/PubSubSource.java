package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.converter.PubSubToRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PubSubSource implements SourceModule {

    private class PubSubSourceParameters {

        private String topic;
        private String subscription;
        private String format;
        private String idAttribute;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getSubscription() {
            return subscription;
        }

        public void setSubscription(String subscription) {
            this.subscription = subscription;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public void setIdAttribute(String idAttribute) {
            this.idAttribute = idAttribute;
        }

    }

    public String getName() { return "pubsub"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        boolean isStreaming = begin.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming();

        if (isStreaming) {
            return Collections.singletonMap(config.getName(), PubSubSource.stream(begin, config));
        } else {
            throw new IllegalArgumentException("PubSubSource only support streaming mode.");
        }
    }

    public static FCollection<GenericRecord> stream(final PBegin begin, final SourceConfig config) {
        final PubSubStream stream = new PubSubStream(config);
        final PCollection<GenericRecord> outputAvro = begin.apply(config.getName(), stream);
        return FCollection.of(config.getName(), outputAvro, DataType.AVRO, stream.schema);
    }

    public static class PubSubStream extends PTransform<PBegin, PCollection<GenericRecord>> {

        private final SourceConfig config;
        private final PubSubSourceParameters parameters;

        private Schema schema;

        private PubSubStream(final SourceConfig config) {
            this.config = config;
            this.parameters = new Gson().fromJson(config.getParameters(), PubSubSourceParameters.class);
        }

        public PCollection<GenericRecord> expand(final PBegin begin) {

            PubsubIO.Read read;
            if("avro".equals(parameters.getFormat())) {
                this.schema = SourceConfig.convertAvroSchema(config.getSchema());
                read = PubsubIO.readAvroGenericRecords(schema);
            } else if("message".equals(parameters.getFormat())) {
                this.schema = PubSubToRecordConverter.createMessageSchema();
                read = PubsubIO.readMessagesWithAttributesAndMessageId();
            } else {
                throw new IllegalArgumentException("PubSubSource must be set format avro or message. " + parameters.getFormat());
            }

            if(config.getTimestampAttribute() != null) {
                read = read.withTimestampAttribute(config.getTimestampAttribute());
            }

            if(parameters.getTopic() != null) {
                read = read.fromTopic(parameters.getTopic());
            } else if(parameters.getSubscription() != null) {
                read = read.fromSubscription(parameters.getSubscription());
            }

            if(parameters.getIdAttribute() != null) {
                read = read.withIdAttribute(parameters.getIdAttribute());
            }

            if("avro".equals(parameters.getFormat())) {
                return begin.apply("ReadPubSubRecord", (PubsubIO.Read<GenericRecord>)read);
            } else if("message".equals(parameters.getFormat())) {
                return begin
                        .apply("ReadPubSubMessage", (PubsubIO.Read<PubsubMessage>)read)
                        .apply("MessageToRecord", ParDo.of(new MessageToRecordDoFn()))
                        .setCoder(AvroCoder.of(schema));
            } else {
                throw new IllegalArgumentException();
            }

        }

    }

    private static class MessageToRecordWithSchemaDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private transient Schema schema;
        private transient BinaryDecoder decoder;
        private transient DatumReader<GenericRecord> reader;

        @Setup
        public void setup() {
            this.schema = PubSubToRecordConverter.createMessageSchema();
        }

        @StartBundle
        public void startBundle(StartBundleContext c) {
            this.decoder = null;
            this.reader = new GenericDatumReader<>(schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            this.decoder = DecoderFactory.get().binaryDecoder(c.element().getPayload(), decoder);
            GenericRecord record = new GenericData.Record(schema);;
            record = reader.read(record, decoder);
            c.output(record);
        }
    }

    private static class MessageToRecordDoFn extends DoFn<PubsubMessage, GenericRecord> {

        private transient Schema schema;

        @Setup
        public void setup() {
            this.schema = PubSubToRecordConverter.createMessageSchema();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(PubSubToRecordConverter.convertMessage(schema, c.element()));
        }

    }

}
