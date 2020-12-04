package com.mercari.solution.util.converter;

import ai.onnxruntime.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.nio.FloatBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class RowToONNXTensorConverter {

    public static Map<String, OnnxTensor> convert(final OrtEnvironment environment, final Collection<NodeInfo> inputInfo, final Row row) {
        //NodeInfo n;
        /*
        return inputInfo.stream().collect(Collectors.toMap(
                NodeInfo::getName,
                nodeInfo -> convert(environment, nodeInfo.getInfo(), row.getSchema().getField(nodeInfo.getName()), row.getValue(nodeInfo.getName()))));
                */
        return null;
    }

    private static OnnxTensor convert(final OrtEnvironment environment, final NodeInfo nodeInfo, final Row row) {
        final String nodeName = nodeInfo.getName();
        final ValueInfo valueInfo = nodeInfo.getInfo();
        if(!row.getSchema().hasField(nodeName)) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(nodeName);

        try {
            if (valueInfo instanceof TensorInfo) {
                final TensorInfo tensorInfo = (TensorInfo) valueInfo;
                final long[] shape = tensorInfo.getShape();
                shape[0] = 1;
                switch (tensorInfo.type) {
                    case STRING:
                        String[] a = {};
                        return OnnxTensor.createTensor(environment, a, shape);
                    case BOOL:
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                        float[] f = {0F};
                        return OnnxTensor.createTensor(environment, FloatBuffer.wrap(f), shape);
                    case DOUBLE:
                    case UNKNOWN:
                    default:
                }
            } else if (valueInfo instanceof MapInfo) {
                final MapInfo mapInfo = (MapInfo) valueInfo;
            } else if (valueInfo instanceof SequenceInfo) {
                final SequenceInfo sequenceInfo = (SequenceInfo) valueInfo;
                if(sequenceInfo.isSequenceOfMaps()) {
                    final MapInfo mapInfo = sequenceInfo.mapInfo;
                } else {
                    switch (sequenceInfo.sequenceType) {
                        case UNKNOWN:
                    }
                }
            }
            return null;
        } catch (OrtException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object convertToTensor(final OrtEnvironment env,
                                              final Schema.FieldType fieldType,
                                              final OnnxJavaType javaType,
                                              final long[] shape,
                                              final Object value) {

        switch (fieldType.getTypeName()) {
            case BOOLEAN:
            case STRING:
                final String str = (String)value;
                switch (javaType) {
                    case BOOL:
                        boolean[][] bs = {{true}};
                        return bs;
                }
                return null;
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case MAP:
            case ARRAY:
            case LOGICAL_TYPE:
            case DATETIME:
            case BYTES:
            case BYTE:
            case ITERABLE:
            case ROW:
            case DECIMAL:
            default:
                return null;
        }
    }

}
