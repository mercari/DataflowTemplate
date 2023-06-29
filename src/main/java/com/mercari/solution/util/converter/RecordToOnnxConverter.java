package com.mercari.solution.util.converter;

import ai.onnxruntime.*;
import com.mercari.solution.util.OnnxUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.bytedeco.onnx.*;

import java.nio.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordToOnnxConverter {

    public static ModelProto convertSchema(final Schema schema) {
        //ModelProto model;
        //model.graph().inp

        for(Schema.Field field : schema.getFields()) {

        }

        TypeProto_Tensor tensorInput = new TypeProto_Tensor();
        tensorInput.set_elem_type(7);
        TensorShapeProto shape = new TensorShapeProto();
        shape.add_dim().setNull();//.set_dim_value(1);
        tensorInput.set_allocated_shape(shape);
        TypeProto typeInput = new TypeProto();
        //typeInput.set_denotation("");
        typeInput.set_allocated_tensor_type(tensorInput);
        ValueInfoProto infoInput = new ValueInfoProto();
        //infoInput.set_name("input1");
        infoInput.set_allocated_type(typeInput);


        return null;
    }

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
                tensors.put(entry.getKey(), convertTensor(environment, tensorInfo, values));
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

    private static OnnxTensor convertTensor(
            final OrtEnvironment environment, final TensorInfo tensorInfo, final List<Object> values)
            throws OrtException {

        final long[] shape = new long[tensorInfo.getShape().length];
        for(int idx = 0; idx < shape.length; idx++) {
            shape[idx] = tensorInfo.getShape()[idx] >= 0 ? tensorInfo.getShape()[idx] : values.size();
        }
        final List<Object> flattenValues = OnnxUtil.flatten(values, shape.length);
        switch (tensorInfo.type) {
            case STRING: {
                final String[] stringValues = new String[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    stringValues[i] = (String) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, stringValues, shape);
            }
            case BOOL: {
                final int[] intValues = new int[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    intValues[i] = (Boolean) flattenValues.get(i) ? 1 : 0;
                }
                return OnnxTensor.createTensor(environment, IntBuffer.wrap(intValues), shape);
            }
            case INT8: {
                final byte[] byteValues = new byte[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    byteValues[i] = (Byte) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, ByteBuffer.wrap(byteValues), shape);
            }
            case INT16: {
                final short[] shortValues = new short[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    shortValues[i] = (Short) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, ShortBuffer.wrap(shortValues), shape);
            }
            case INT32: {
                final int[] intValues = new int[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    intValues[i] = (Integer) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, IntBuffer.wrap(intValues), shape);
            }
            case INT64: {
                final long[] longValues = new long[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    longValues[i] = (Long) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, LongBuffer.wrap(longValues), shape);
            }
            case FLOAT: {
                final float[] floatValues = new float[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    floatValues[i] = (Float) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, FloatBuffer.wrap(floatValues), shape);
            }
            case DOUBLE: {
                final double[] doubleValues = new double[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    doubleValues[i] = (Double) flattenValues.get(i);
                }
                return OnnxTensor.createTensor(environment, DoubleBuffer.wrap(doubleValues), shape);
            }
            case UNKNOWN:
            default:
                throw new IllegalArgumentException();
        }

    }

}
