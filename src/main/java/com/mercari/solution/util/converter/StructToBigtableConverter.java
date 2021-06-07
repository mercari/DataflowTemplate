package com.mercari.solution.util.converter;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.sink.BigtableSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StructToBigtableConverter {

    public static Iterable<Mutation> convert(final Type type,
                                             final Struct struct,
                                             final String defaultColumnFamily,
                                             final BigtableSink.Format defaultFormat,
                                             final Map<String, BigtableSink.ColumnSetting> columnSettings) {

        final List<Mutation> mutations = new ArrayList<>();
        for(final Type.StructField field : type.getStructFields()) {
            if(struct.isNull(field.getName())) {
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
                    bytes = StructSchemaUtil.getAsByteString(struct, field.getName());
                    break;
                }
                case string: {
                    final String stringValue = StructSchemaUtil.getAsString(struct, field.getName());
                    if(stringValue == null) {
                        bytes = null;
                    } else {
                        bytes = ByteString.copyFrom(stringValue, StandardCharsets.UTF_8);
                    }
                    break;
                }
                case avro: {
                    if(field.getType().getCode().equals(Type.Code.STRUCT)) {
                        final Struct fieldStruct = struct.getStruct(field.getName());
                        final org.apache.avro.Schema fieldSchema = StructToRecordConverter.convertSchema(fieldStruct.getType());
                        final GenericRecord fieldRecord = StructToRecordConverter.convert(fieldSchema, fieldStruct);
                        try {
                            bytes = ByteString.copyFrom(AvroSchemaUtil.encode(fieldRecord));
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    } else {
                        bytes = StructSchemaUtil.getAsByteString(struct, field.getName());
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
