package com.mercari.solution.debug;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class AvroViewer {

    @Test
    public void view() throws IOException {
        // set avro file under src/test/resources and set file name to var 'avroFileName'
        final String avroFileName = "xxx.avro";
        final URL uri = ClassLoader.getSystemResource(avroFileName);
        final File avroFile = new File(uri.getPath());
        printAvroBinary(avroFile);
        final List<GenericRecord> records = getGenericRecord(avroFile);
        for(final GenericRecord r : records) {
            System.out.println(r);
        }
    }

    private static List<GenericRecord> getGenericRecord(final File file) throws IOException {
        final DatumReader<GenericRecord> dataReader = new GenericDatumReader<>();
        final DataFileReader<GenericRecord> recordReader = new DataFileReader<>(file, dataReader);
        final Schema schema = ((GenericDatumReader<GenericRecord>) dataReader).getSchema();
        System.out.println(schema);
        final List<GenericRecord> records = new ArrayList<>();
        while (recordReader.hasNext()) {
            final GenericRecord record = recordReader.next();
            records.add(record);
        }
        return records;
    }

    private static void printAvroBinary(final File file) throws IOException {
        try(final FileInputStream fis = new FileInputStream(file);
            final InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            final BufferedReader br = new BufferedReader(isr)) {

            final StringBuilder sb = new StringBuilder();
            br.lines().forEach(line -> sb.append(line + '\n'));
            System.out.println(sb.toString());
        }
    }

}
