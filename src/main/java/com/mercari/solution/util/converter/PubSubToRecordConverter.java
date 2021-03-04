package com.mercari.solution.util.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class PubSubToRecordConverter {

    public static GenericRecord convertMessage(final Schema schema, final PubsubMessage message) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("payload", message.getPayload());
        builder.set("messageId", message.getMessageId());
        builder.set("attributes", message.getAttributeMap());
        return builder.build();
    }

    public static Schema createMessageSchema() {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        schemaFields.name("payload")
                .type(AvroSchemaUtil.NULLABLE_BYTES)
                .noDefault();
        schemaFields.name("messageId")
                .type(AvroSchemaUtil.NULLABLE_STRING)
                .noDefault();
        schemaFields.name("attributes")
                .type(AvroSchemaUtil.NULLABLE_MAP_STRING)
                .noDefault();
        return schemaFields.endRecord();
    }

}
