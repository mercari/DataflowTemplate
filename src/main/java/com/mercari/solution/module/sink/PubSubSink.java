package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.converter.DataTypeTransform;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PubSubSink {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

    private class PubSubSinkParameters implements Serializable {

        private String topic;
        private String format;
        private List<String> attributes;
        private String idAttribute;
        private String timestampAttribute;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<String> attributes) {
            this.attributes = attributes;
        }

        public String getIdAttribute() {
            return idAttribute;
        }

        public void setIdAttribute(String idAttribute) {
            this.idAttribute = idAttribute;
        }

        public String getTimestampAttribute() {
            return timestampAttribute;
        }

        public void setTimestampAttribute(String timestampAttribute) {
            this.timestampAttribute = timestampAttribute;
        }

    }

    private enum Format {
        avro,
        json
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final PubSubSinkParameters parameters = new Gson().fromJson(config.getParameters(), PubSubSinkParameters.class);
        final Write write = new Write(collection, parameters, waits);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return FCollection.update(write.collection, output);
    }

    public static class Write extends PTransform<PCollection<?>, PCollection<Void>> {

        private static final Logger LOG = LoggerFactory.getLogger(Write.class);

        private FCollection<?> collection;
        private final PubSubSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private Write(final FCollection<?> collection,
                      final PubSubSinkParameters parameters,
                      final List<FCollection<?>> waits) {

            this.collection = collection;
            this.parameters = parameters;
            this.waits = waits;
        }

        public PCollection<Void> expand(final PCollection<?> input) {

            validateParameters();
            setDefaultParameters();

            final PCollection<GenericRecord> records = input
                    .apply("ToMutation", DataTypeTransform.transform(collection, DataType.AVRO));

            PCollection<PubsubMessage> messages = null;

            PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(parameters.getTopic());

            if(parameters.getIdAttribute() != null) {
                write = write.withIdAttribute(parameters.getIdAttribute());
            }
            if(parameters.getTimestampAttribute() != null) {
                write = write.withTimestampAttribute(parameters.getTimestampAttribute());
            }

            return null;
        }

        private void validateParameters() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getTopic() == null) {
                errorMessages.add("Parameter must contain topic");
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if(parameters.getAttributes() == null) {
                parameters.setAttributes(new ArrayList<>());
            }
        }

        private class PubsubMessageDoFn extends DoFn<GenericRecord, PubsubMessage> {

            private final List<String> attributes;
            private final String idAttribute;
            private final String schemaString;

            private transient Schema schema;
            private transient BinaryEncoder encoder;


            public PubsubMessageDoFn(final String schemaString, final List<String> attributes, final String idAttribute) {
                this.schemaString = schemaString;
                this.attributes = attributes;
                this.idAttribute = idAttribute;
            }

            @Setup
            public void setup() {
                this.schema = new Schema.Parser().parse(schemaString);
            }

            @StartBundle
            public void startBundle(final StartBundleContext c) {
                this.encoder = null;
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                final GenericRecord record = c.element();

                final Map<String, String> attributeMap;
                if(attributes.size() > 0) {
                    attributeMap = new HashMap<>();
                    for(final String attribute : attributes) {
                        final Object value = record.get(attribute);
                        if(value != null) {
                            attributeMap.put(attribute, value.toString());
                        }
                    }
                } else {
                    attributeMap = null;
                }

                final String attributeId;
                if(idAttribute != null) {
                    final Object value = record.get(idAttribute);
                    if(value != null) {
                        attributeId = value.toString();
                    } else {
                        attributeId = null;
                    }
                } else {
                    attributeId = null;
                }

                try(final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    this.encoder = EncoderFactory.get().binaryEncoder(out, this.encoder);
                    final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                    writer.write(record, encoder);
                    encoder.flush();
                    final byte[] payload = out.toByteArray();
                    final PubsubMessage message = new PubsubMessage(payload, attributeMap, attributeId);
                    c.output(message);
                }
            }

        }
    }
}