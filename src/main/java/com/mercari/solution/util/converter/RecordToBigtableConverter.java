package com.mercari.solution.util.converter;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.*;
import com.mercari.solution.module.sink.BigtableSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class RecordToBigtableConverter {

    public static Iterable<Mutation> convert(final Schema schema,
                                             final GenericRecord record,
                                             final String defaultColumnFamily,
                                             final BigtableSink.Format defaultFormat,
                                             final Map<String, BigtableSink.ColumnSetting> columnSettings) {

        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            final Object value = record.get(field.name());
            if(value == null) {
                continue;
            }

            final BigtableSink.ColumnSetting columnSetting = columnSettings.getOrDefault(field.name(), null);

            final String columnFamily;
            final String columnQualifier;
            final BigtableSink.Format format;
            if(columnSetting == null) {
                columnFamily = defaultColumnFamily;
                columnQualifier = field.name();
                format = defaultFormat;
            } else {
                columnFamily = columnSetting.getColumnFamily();
                columnQualifier = columnSetting.getColumnQualifier();
                format = columnSetting.getFormat();
                if(columnSetting.getExclude() != null && columnSetting.getExclude()) {
                    continue;
                }
            }

            final ByteString bytes;
            switch (format) {
                case bytes: {
                    bytes = AvroSchemaUtil.getAsByteString(record, field.name());
                    break;
                }
                case string: {
                    final String stringValue = AvroSchemaUtil.getAsString(record, field.name());
                    if(stringValue == null) {
                        bytes = null;
                    } else {
                        bytes = ByteString.copyFrom(stringValue, StandardCharsets.UTF_8);
                    }
                    break;
                }
                case avro: {
                    if(AvroSchemaUtil.unnestUnion(field.schema()).getType().equals(Schema.Type.RECORD)) {
                        final GenericRecord fieldRecord = (GenericRecord) value;
                        try {
                            bytes = ByteString.copyFrom(AvroSchemaUtil.encode(fieldRecord));
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    } else {
                        bytes = AvroSchemaUtil.getAsByteString(record, field.name());
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }

            if(bytes == null) {
                continue;
            }

            final Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily)
                    .setColumnQualifier(ByteString.copyFrom(columnQualifier, StandardCharsets.UTF_8))
                    .setValue(bytes)
                    .build();
            final Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
            mutations.add(mutation);
        }
        return mutations;
    }

}
