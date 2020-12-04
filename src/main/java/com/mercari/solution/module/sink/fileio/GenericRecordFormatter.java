package com.mercari.solution.module.sink.fileio;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

public interface GenericRecordFormatter<InputT>  extends Serializable {
    GenericRecord formatRecord(Schema schema, InputT element);
}
