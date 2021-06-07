package com.mercari.solution.util.converter;

import com.google.bigtable.v2.Mutation;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.mercari.solution.module.sink.BigtableSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EntityToBigtableConverter {

    public static Iterable<Mutation> convert(final Schema schema,
                                             final Entity entity,
                                             final String defaultColumnFamily,
                                             final BigtableSink.Format defaultFormat,
                                             final Map<String, BigtableSink.ColumnSetting> columnSettings) {

        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            if(!schema.hasField(field.getName())) {
                continue;
            }
            final Value value = entity.getPropertiesOrDefault(field.getName(), null);
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
                    bytes = EntitySchemaUtil.getAsByteString(entity, field.getName());
                    break;
                }
                case string: {
                    final String stringValue = EntitySchemaUtil.getAsString(entity, field.getName());
                    if(stringValue == null) {
                        bytes = null;
                    } else {
                        bytes = ByteString.copyFrom(stringValue, StandardCharsets.UTF_8);
                    }
                    break;
                }
                case avro: {
                    if(field.getType().getTypeName().equals(Schema.TypeName.ROW)) {
                        final Entity fieldEntity = value.getEntityValue();
                        final org.apache.avro.Schema fieldSchema = RowToRecordConverter.convertSchema(field.getType().getRowSchema());
                        final GenericRecord fieldRecord = EntityToRecordConverter.convert(fieldSchema, fieldEntity);
                        try {
                            bytes = ByteString.copyFrom(AvroSchemaUtil.encode(fieldRecord));
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    } else {
                        bytes = EntitySchemaUtil.getAsByteString(entity, field.getName());
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
