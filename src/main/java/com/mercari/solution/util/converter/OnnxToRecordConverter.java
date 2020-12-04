package com.mercari.solution.util.converter;

import ai.onnxruntime.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OnnxToRecordConverter {

    public static Schema convertSchema(final Map<String, NodeInfo> outputs) {
        return null;
    }

    public static List<GenericRecord> convert(final OrtSession.Result result) {

        final Iterator<Map.Entry<String, OnnxValue>> iterator = result.iterator();
        while(iterator.hasNext()) {
            final Map.Entry<String, OnnxValue> entry = iterator.next();
            if(entry.getValue() instanceof OnnxTensor) {
                //final TensorInfo tensorInfo = (TensorInfo) entry.getValue().getInfo();
                final OnnxTensor tensor = (OnnxTensor) entry.getValue();
                //tensorInfo.getShape();
            } else if(entry.getValue().getInfo() instanceof MapInfo) {
                final MapInfo mapInfo = (MapInfo) entry.getValue().getInfo();
            } else if(entry.getValue().getInfo() instanceof SequenceInfo) {
                final SequenceInfo sequenceInfo = (SequenceInfo) entry.getValue().getInfo();
            } else {
                throw new IllegalArgumentException("Not supported node type: " + entry.getValue().getInfo());
            }
        }

        return null;
    }

}
