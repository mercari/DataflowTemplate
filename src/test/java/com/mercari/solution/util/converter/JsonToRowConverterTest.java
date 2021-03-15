package com.mercari.solution.util.converter;

import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class JsonToRowConverterTest {

    @Test
    public void test() {
        final Row row = TestDatum.generateRow();
        final String json = RowToJsonConverter.convert(row);
        final Row revertedRow = JsonToRowConverter.convert(row.getSchema(), json);
        testFlatField(revertedRow);
        final Row revertedRowChild = revertedRow.getRow("recordField");
        testFlatField(revertedRowChild);
        final Row revertedRowGrandchild = revertedRowChild.getRow("recordField");
        testFlatField(revertedRowGrandchild);

        for(final Row child : revertedRow.<Row>getArray("recordArrayField")) {
            testFlatField(child);
        }

        for(final Row child : revertedRowChild.<Row>getArray("recordArrayField")) {
            testFlatField(child);
        }
    }

    private void testFlatField(final Row row) {
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), row.getBoolean("booleanField"));
        Assert.assertEquals(TestDatum.getStringFieldValue(), row.getString("stringField"));
        Assert.assertEquals(TestDatum.getBytesFieldValue(), new String(row.getBytes("bytesField"), StandardCharsets.UTF_8));
        Assert.assertEquals(TestDatum.getIntFieldValue(), row.getInt32("intField"));
        Assert.assertEquals(TestDatum.getLongFieldValue(), row.getInt64("longField"));
        Assert.assertEquals(TestDatum.getFloatFieldValue(), row.getFloat("floatField"));
        Assert.assertEquals(TestDatum.getDoubleFieldValue(), row.getDouble("doubleField"));
        Assert.assertEquals(TestDatum.getDateFieldValue(),row.getValue("dateField"));
        Assert.assertEquals(TestDatum.getTimestampFieldValue(), row.getDateTime("timestampField"));
        Assert.assertEquals(TestDatum.getDecimalFieldValue(), row.getDecimal("decimalField"));
        // TODO array check
    }

}
