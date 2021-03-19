package com.mercari.solution.util.schema;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EntitySchemaUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testSelectFields() {
        final Row row = TestDatum.generateRow();
        final List<String> fields = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");

        final Schema schema = RowSchemaUtil.selectFields(row.getSchema(), fields);
        final Entity entity = TestDatum.generateEntity();

        // entity test
        final Entity selectedEntity = EntitySchemaUtil.toBuilder(schema, entity).build();
        Assert.assertEquals(5, selectedEntity.getPropertiesCount());
        Assert.assertEquals(TestDatum.getStringFieldValue(), selectedEntity.getPropertiesOrThrow("stringField").getStringValue());
        Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), selectedEntity.getPropertiesOrThrow("intField").getIntegerValue());
        Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), selectedEntity.getPropertiesOrThrow("longField").getIntegerValue());

        final Entity selectedEntityChild = selectedEntity.getPropertiesOrThrow("recordField").getEntityValue();
        Assert.assertEquals(5, selectedEntityChild.getPropertiesCount());
        Assert.assertEquals(TestDatum.getStringFieldValue(), selectedEntityChild.getPropertiesOrThrow("stringField").getStringValue());
        Assert.assertEquals(TestDatum.getDoubleFieldValue().doubleValue(), selectedEntityChild.getPropertiesOrThrow("doubleField").getDoubleValue(), DELTA);
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedEntityChild.getPropertiesOrThrow("booleanField").getBooleanValue());

        final Entity selectedEntityGrandchild = selectedEntityChild.getPropertiesOrThrow("recordField").getEntityValue();
        Assert.assertEquals(2, selectedEntityGrandchild.getPropertiesCount());
        Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), selectedEntityGrandchild.getPropertiesOrThrow("intField").getIntegerValue());
        Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), selectedEntityGrandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);

        Assert.assertEquals(2, selectedEntity.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
        for(final Value childValue : selectedEntity.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
            final Entity child = childValue.getEntityValue();
            Assert.assertEquals(4, child.getPropertiesCount());
            Assert.assertEquals(TestDatum.getStringFieldValue(), child.getPropertiesOrThrow("stringField").getStringValue());
            Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(),
                    Timestamps.toMillis(child.getPropertiesOrThrow("timestampField").getTimestampValue()));

            Assert.assertEquals(2, child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
            for(final Value grandchilValue : child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
                final Entity grandchild = grandchilValue.getEntityValue();
                Assert.assertEquals(2, grandchild.getPropertiesCount());
                Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), grandchild.getPropertiesOrThrow("intField").getIntegerValue());
                Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), grandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
            }

            final Entity grandchild = child.getPropertiesOrThrow("recordField").getEntityValue();
            Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), grandchild.getPropertiesOrThrow("intField").getIntegerValue());
            Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), grandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
        }

        // null fields row test
        final Row rowNull = TestDatum.generateRowNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Schema schemaNull = RowSchemaUtil.selectFields(rowNull.getSchema(), newFields);

        final Entity entityNull = TestDatum.generateEntityNull();
        final Entity selectedEntityNull = EntitySchemaUtil.toBuilder(schemaNull, entityNull).build();
        Assert.assertEquals(7, selectedEntityNull.getPropertiesCount());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("stringField").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("intField").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("longField").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("recordFieldNull").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("recordArrayFieldNull").getNullValue());

        final Entity selectedEntityChildNull = selectedEntityNull.getPropertiesOrThrow("recordField").getEntityValue();
        Assert.assertEquals(5, selectedEntityChildNull.getPropertiesCount());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityChildNull.getPropertiesOrThrow("stringField").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityChildNull.getPropertiesOrThrow("doubleField").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityChildNull.getPropertiesOrThrow("booleanField").getNullValue());

        final Entity selectedEntityGrandchildNull = selectedEntityChildNull.getPropertiesOrThrow("recordField").getEntityValue();
        Assert.assertEquals(2, selectedEntityGrandchildNull.getPropertiesCount());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityGrandchildNull.getPropertiesOrThrow("intField").getNullValue());
        Assert.assertEquals(NullValue.NULL_VALUE, selectedEntityGrandchildNull.getPropertiesOrThrow("floatField").getNullValue());

        Assert.assertEquals(2, selectedEntityNull.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
        for(final Value childValue : selectedEntityNull.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
            final Entity child = childValue.getEntityValue();
            Assert.assertEquals(4, child.getPropertiesCount());
            Assert.assertEquals(NullValue.NULL_VALUE, child.getPropertiesOrThrow("stringField").getNullValue());
            Assert.assertEquals(NullValue.NULL_VALUE, child.getPropertiesOrThrow("timestampField").getNullValue());

            Assert.assertEquals(2, child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
            for(final Value grandchildValue : child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
                final Entity grandchild = grandchildValue.getEntityValue();
                Assert.assertEquals(2, grandchild.getPropertiesCount());
                Assert.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("intField").getNullValue());
                Assert.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("floatField").getNullValue());
            }

            final Entity grandchild = child.getPropertiesOrThrow("recordField").getEntityValue();
            Assert.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("intField").getNullValue());
            Assert.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("floatField").getNullValue());
        }

    }

}
