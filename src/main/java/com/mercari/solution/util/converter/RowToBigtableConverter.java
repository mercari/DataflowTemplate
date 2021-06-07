package com.mercari.solution.util.converter;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.*;
import com.mercari.solution.module.sink.BigtableSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class RowToBigtableConverter {

    public static Iterable<Mutation> convert(final Schema schema,
                                             final Row row,
                                             final String defaultColumnFamily,
                                             final BigtableSink.Format defaultFormat,
                                             final Map<String, BigtableSink.ColumnSetting> columnSettings) {

        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            if(!schema.hasField(field.getName())) {
                continue;
            }
            final Object value = row.getValue(field.getName());
            if(value == null) {
                continue;
            }

            final BigtableSink.ColumnSetting columnSetting = columnSettings.getOrDefault(field.getName(), null);

            final String columnFamily;
            final String columnQualifier;
            final BigtableSink.Format format;
            if(columnSetting == null) {
                columnFamily = defaultColumnFamily;
                columnQualifier = field.getName();
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
                    bytes = RowSchemaUtil.getAsByteString(row, field.getName());
                    break;
                }
                case string: {
                    final String stringValue = RowSchemaUtil.getAsString(row, field.getName());
                    if(stringValue == null) {
                        bytes = null;
                    } else {
                        bytes = ByteString.copyFrom(stringValue, StandardCharsets.UTF_8);
                    }
                    break;
                }
                case avro: {
                    if(field.getType().getTypeName().equals(Schema.TypeName.ROW)) {
                        final Row fieldRow = row.getRow(field.getName());
                        final org.apache.avro.Schema fieldSchema = RowToRecordConverter.convertSchema(fieldRow.getSchema());
                        final GenericRecord fieldRecord = RowToRecordConverter.convert(fieldSchema, fieldRow);
                        try {
                            bytes = ByteString.copyFrom(AvroSchemaUtil.encode(fieldRecord));
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    } else {
                        bytes = RowSchemaUtil.getAsByteString(row, field.getName());
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
