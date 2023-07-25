package com.mercari.solution.util.converter;

import ai.onnxruntime.*;
import com.mercari.solution.util.domain.ml.ONNXRuntimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordToOnnxConverter {

    public static Map<String, OnnxTensor> convert(
            final OrtEnvironment environment, final OrtSession session, final List<GenericRecord> records)
            throws OrtException {

        final Map<String, OnnxTensor> tensors = new HashMap<>();
        for(final Map.Entry<String, NodeInfo> entry : session.getInputInfo().entrySet()) {
            if(entry.getValue().getInfo() instanceof TensorInfo) {
                final TensorInfo tensorInfo = (TensorInfo) entry.getValue().getInfo();
                final List<Object> values = records.stream()
                        .map(r -> r.get(entry.getKey()))
                        .collect(Collectors.toList());
                tensors.put(entry.getKey(), ONNXRuntimeUtil.convertTensor(environment, tensorInfo, values));
            } else if(entry.getValue().getInfo() instanceof MapInfo) {
                final MapInfo mapInfo = (MapInfo) entry.getValue().getInfo();
            } else if(entry.getValue().getInfo() instanceof SequenceInfo) {
                final SequenceInfo sequenceInfo = (SequenceInfo) entry.getValue().getInfo();
            } else {
                throw new IllegalArgumentException("Not supported node type: " + entry.getValue().getInfo());
            }
        }

        return tensors;
    }

    public static Object getValue(final TensorInfo tensorInfo, final String field, final GenericRecord record, final Object defaultValue) {
        final Object value = record.get(field);
        if(value == null) {
            return defaultValue;
        }
        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(record.getSchema().getField(field).schema());
        switch (fieldSchema.getType()) {
            case ARRAY: {
                return ((List<Object>) value).stream()
                        .map(v -> getValue(tensorInfo, fieldSchema.getElementType(), v))
                        .collect(Collectors.toList());
            }
            case RECORD: {
                throw new IllegalArgumentException();
            }
            default:
                return getValue(tensorInfo, fieldSchema, value);
        }

    }

    private static Object getValue(final TensorInfo tensorInfo, final Schema fieldSchema, final Object value) {
        switch (fieldSchema.getType()) {
            case BYTES: {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                switch (tensorInfo.type) {
                    case INT8:
                        return byteBuffer.array();
                    case UINT8:
                    case INT16:
                        return byteBuffer.array();
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
                }
            }
            case ENUM:
            case STRING: {
                final String stringValue = value.toString();
                switch (tensorInfo.type) {
                    case STRING:
                        return stringValue;
                    case BOOL:
                        return Boolean.valueOf(stringValue);
                    case INT32:
                        return Integer.valueOf(stringValue);
                    case INT64:
                        return Long.valueOf(stringValue);
                    case FLOAT:
                        return Float.valueOf(stringValue);
                    case DOUBLE:
                        return Double.valueOf(stringValue);
                    case INT8:
                        return Byte.valueOf(stringValue);
                    case INT16:
                        return Short.valueOf(stringValue);
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
                }
            }
            case INT: {
                final Integer intValue = (Integer) value;
                switch (tensorInfo.type) {
                    case BOOL:
                        return intValue > 0;
                    case INT32:
                        return intValue;
                    case INT64:
                        return intValue.longValue();
                    case FLOAT:
                        return intValue.floatValue();
                    case DOUBLE:
                        return intValue.doubleValue();
                    case INT8:
                        return intValue.byteValue();
                    case INT16:
                        return intValue.shortValue();
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
                }
            }
            case LONG: {
                final Long longValue = (Long) value;
                switch (tensorInfo.type) {
                    case BOOL:
                        return longValue > 0;
                    case INT32:
                        return longValue.intValue();
                    case INT64:
                        return longValue;
                    case FLOAT:
                        return longValue.floatValue();
                    case DOUBLE:
                        return longValue.doubleValue();
                    case INT8:
                        return longValue.byteValue();
                    case INT16:
                        return longValue.shortValue();
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
                }
            }
            case FLOAT: {
                final Float floatValue = (Float) value;
                switch (tensorInfo.type) {
                    case BOOL:
                        return floatValue > 0;
                    case INT32:
                        return floatValue.intValue();
                    case INT64:
                        return floatValue.longValue();
                    case FLOAT:
                        return floatValue;
                    case DOUBLE:
                        return floatValue.doubleValue();
                    case INT8:
                        return floatValue.byteValue();
                    case INT16:
                        return floatValue.shortValue();
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
                }
            }
            case DOUBLE: {
                final Double doubleValue = (Double) value;
                switch (tensorInfo.type) {
                    case BOOL:
                        return doubleValue > 0;
                    case INT32:
                        return doubleValue.intValue();
                    case INT64:
                        return doubleValue.longValue();
                    case FLOAT:
                        return doubleValue.floatValue();
                    case DOUBLE:
                        return doubleValue;
                    case INT8:
                        return doubleValue.byteValue();
                    case INT16:
                        return doubleValue.shortValue();
                    case UNKNOWN:
                    default:
                        throw new IllegalArgumentException("Not supported field type: " + fieldSchema.getType());
                }
            }
            default:
                throw new IllegalArgumentException();
        }
    }

}
