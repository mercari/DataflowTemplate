/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonArray;
import com.mercari.solution.util.gcp.SpannerUtil;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converter converts Cloud Spanner Struct to Cloud Spanner Mutation
 */
public class StructToMutationConverter {

    private static final String DEFAULT_PREFIX = "autoprefix";

    private StructToMutationConverter() {}

    public static Mutation convert(final Struct struct,
                                   final String table, final String mutationOp, final Iterable<String> keyFields) {

        return convert(struct, table, mutationOp, keyFields, null, null, DEFAULT_PREFIX);
    }

    public static Mutation convert(final Struct struct,
                                   final String table, final String mutationOp, final Iterable<String> keyFields,
                                   final Set<String> excludeFields, final Set<String> hideFields) {

        return convert(struct, table, mutationOp, keyFields, excludeFields, hideFields, DEFAULT_PREFIX);
    }

    // For DataTypeTransform.SpannerMutationDoFn interface
    public static Mutation convert(final Schema schema, final Struct struct,
                                   final String table, final String mutationOp, final Iterable<String> keyFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {

        return convert(struct, table, mutationOp, keyFields, excludeFields, hideFields, DEFAULT_PREFIX);
    }

    /**
     * Convert Spanner {@link Struct} object to Spanner {@link Mutation} object.
     *
     * @param struct Spanner Struct to be converted to Mutation object.
     * @param table Spanner table name to store.
     * @param mutationOp Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE.
     * @return Spanner Mutation object.
     */
    public static Mutation convert(final Struct struct,
                                   final String table, final String mutationOp, final Iterable<String> keyFields,
                                   final Set<String> excludeFields, final Set<String> hideFields,
                                   final String fieldPrefix) {

        if(mutationOp != null && "DELETE".equals(mutationOp.trim().toUpperCase())) {
            return delete(struct, table, keyFields);
        }

        Mutation.WriteBuilder builder = SpannerUtil.createMutationWriteBuilder(table, mutationOp);
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }

            final boolean isNullField = struct.isNull(field.getName());
            final String fieldName;
            if(field.getName().startsWith("_")) {
                fieldName = fieldPrefix + field.getName();
            } else {
                fieldName = field.getName();
            }
            switch(field.getType().getCode()) {
                case STRING:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getString(field.getName()));
                    break;
                case BYTES:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getBytes(field.getName()));
                    break;
                case BOOL:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getBoolean(field.getName()));
                    break;
                case INT64:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getLong(field.getName()));
                    break;
                case FLOAT64:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getDouble(field.getName()));
                    break;
                case DATE:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getDate(field.getName()));
                    break;
                case TIMESTAMP:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getTimestamp(field.getName()));
                    break;
                case STRUCT:
                    // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                    // https://cloud.google.com/spanner/docs/data-types
                    if(isNullField) {
                        builder = builder.set(fieldName).to((String) null);
                    } else {
                        Struct child = struct.getStruct(field.getName());
                        final String json = StructToJsonConverter.convert(child);
                        builder = builder.set(fieldName).to(json);
                    }
                    break;
                case ARRAY:
                    switch (field.getType().getArrayElementType().getCode()) {
                        case STRING:
                            builder = builder.set(fieldName).toStringArray(isNullField ? null : struct.getStringList(field.getName()));
                            break;
                        case BYTES:
                            builder = builder.set(fieldName).toBytesArray(isNullField ? null : struct.getBytesList(field.getName()));
                            break;
                        case BOOL:
                            builder = builder.set(fieldName).toBoolArray(isNullField ? null : struct.getBooleanArray(field.getName()));
                            break;
                        case INT64:
                            builder = builder.set(fieldName).toInt64Array(isNullField ? null : struct.getLongArray(field.getName()));
                            break;
                        case FLOAT64:
                            builder = builder.set(fieldName).toFloat64Array(isNullField ? null : struct.getDoubleArray(field.getName()));
                            break;
                        case DATE:
                            builder = builder.set(fieldName).toDateArray(isNullField ? null : struct.getDateList(field.getName()));
                            break;
                        case TIMESTAMP:
                            builder = builder.set(fieldName).toTimestampArray(isNullField ? null : struct.getTimestampList(field.getName()));
                            break;
                        case STRUCT:
                            // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                            // https://cloud.google.com/spanner/docs/data-types
                            if(isNullField) {
                                builder = builder.set(fieldName).to((String) null);
                            } else {
                                List<Struct> children = struct.getStructList(field.getName());
                                final JsonArray array = new JsonArray();
                                children.stream()
                                        .map(StructToJsonConverter::convertStruct)
                                        .forEach(array::add);
                                builder = builder.set(fieldName).to(array.toString());
                            }
                            break;
                        case ARRAY:
                            // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2019/03/04)
                            // https://cloud.google.com/spanner/docs/data-types
                            break;
                    }
            }
        }
        return builder.build();
    }

    public static MutationGroup convertGroup(final Type type, final Struct struct, final String mutationOp, final String primaryField) {
        return convertGroup(struct, mutationOp, primaryField);
    }

    private static MutationGroup convertGroup(final Struct struct, final String mutationOp, final String primaryField) {
        Mutation primary = null;
        final List<Mutation> mutations = new ArrayList<>();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            if(struct.isNull(fieldName)) {
                continue;
            }
            switch (field.getType().getCode()) {
                case BOOL:
                case STRING:
                case BYTES:
                case INT64:
                case FLOAT64:
                case DATE:
                case TIMESTAMP:
                    break;
                case STRUCT:
                    final Mutation mutation = convert(struct, fieldName, mutationOp, null, null, null);
                    if(fieldName.equals(primaryField)) {
                        primary = mutation;
                    } else {
                        mutations.add(mutation);
                    }
                    break;
                case ARRAY: {
                    if (!Type.Code.STRUCT.equals(field.getType().getArrayElementType().getCode())) {
                        break;
                    }
                    final List<Mutation> mutationArray = struct.getStructList(fieldName).stream()
                            .map(s -> convert(s, fieldName, mutationOp, null, null, null))
                            .collect(Collectors.toList());
                    if(mutationArray.size() == 0) {
                        break;
                    }
                    if(fieldName.equals(primaryField)) {
                        primary = mutationArray.get(0);
                        mutations.addAll(mutationArray.subList(1, mutationArray.size()));
                    } else {
                        mutations.addAll(mutationArray);
                    }
                    break;
                }
                default:
                    break;
            }

        }
        if(primary == null) {
            return MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
        }
        return MutationGroup.create(primary, mutations);
    }

    /**
     * Convert Spanner {@link Struct} object to Spanner {@link Mutation} object to delete.
     *
     * @param struct Spanner Struct to be deleted.
     * @param table Spanner table name to delete specified struct object.
     * @param keyFields Key fields in the struct. If composite key case, set comma-separated fields in key sequence.
     * @return Spanner delete Mutation object.
     */
    private static Mutation delete(final Struct struct, final String table, final Iterable<String> keyFields) {
        Key.Builder builder = Key.newBuilder();
        for(final String keyField : keyFields) {
            if(struct.isNull(keyField)) {
                builder = builder.appendObject(null);
                continue;
            }
            switch(struct.getColumnType(keyField).getCode()) {
                case STRING:
                    builder = builder.append(struct.getString(keyField));
                    break;
                case BYTES:
                    builder = builder.append(struct.getBytes(keyField));
                    break;
                case BOOL:
                    builder = builder.append(struct.getBoolean(keyField));
                    break;
                case INT64:
                    builder = builder.append(struct.getLong(keyField));
                    break;
                case FLOAT64:
                    builder = builder.append(struct.getDouble(keyField));
                    break;
                case DATE:
                    builder = builder.append(struct.getDate(keyField));
                    break;
                case TIMESTAMP:
                    builder = builder.append(struct.getTimestamp(keyField));
                    break;
                case STRUCT:
                case ARRAY:
                default:
                    throw new IllegalArgumentException(String.format(
                            "field: %s, fieldType: %s at table %s, is impossible as Key.",
                            keyField, struct.getColumnType(keyField).toString(), table));
            }
        }
        return Mutation.delete(table, builder.build());
    }

}