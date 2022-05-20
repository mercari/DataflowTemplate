package com.mercari.solution.util.schema;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.TestDatum;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class StructSchemaUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testSelectFields() {
        final Struct struct = TestDatum.generateStruct();
        final List<String> fields = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");

        final Type type = StructSchemaUtil.selectFields(struct.getType(), fields);

        // schema test
        Assert.assertEquals(5, type.getStructFields().size());
        Assert.assertTrue(type.getFieldIndex("stringField") >= 0);
        Assert.assertTrue(type.getFieldIndex("intField") >= 0);
        Assert.assertTrue(type.getFieldIndex("longField") >= 0);
        Assert.assertTrue(type.getFieldIndex("recordField") >= 0);
        Assert.assertTrue(type.getFieldIndex("recordArrayField") >= 0);

        final Type typeChild = type.getStructFields().get(type.getFieldIndex("recordField")).getType();
        Assert.assertEquals(5, typeChild.getStructFields().size());
        Assert.assertTrue(typeChild.getFieldIndex("stringField") >= 0);
        Assert.assertTrue(typeChild.getFieldIndex("doubleField") >= 0);
        Assert.assertTrue(typeChild.getFieldIndex("booleanField") >= 0);
        Assert.assertTrue(typeChild.getFieldIndex("recordField") >= 0);
        Assert.assertTrue(typeChild.getFieldIndex("recordArrayField") >= 0);

        Assert.assertEquals(Type.Code.ARRAY, typeChild.getStructFields().get(typeChild.getFieldIndex("recordArrayField")).getType().getCode());
        final Type typeChildChildren = typeChild.getStructFields().get(typeChild.getFieldIndex("recordArrayField")).getType().getArrayElementType();
        Assert.assertEquals(2, typeChildChildren.getStructFields().size());
        Assert.assertTrue(typeChildChildren.getFieldIndex("intField") >= 0);
        Assert.assertTrue(typeChildChildren.getFieldIndex("floatField") >= 0);

        final Type typeGrandchild = typeChild.getStructFields().get(typeChild.getFieldIndex("recordField")).getType();
        Assert.assertEquals(2, typeGrandchild.getStructFields().size());
        Assert.assertTrue(typeGrandchild.getFieldIndex("intField") >= 0);
        Assert.assertTrue(typeGrandchild.getFieldIndex("floatField") >= 0);

        Assert.assertEquals(Type.Code.ARRAY, type.getStructFields().get(type.getFieldIndex("recordArrayField")).getType().getCode());
        final Type typeChildren = type.getStructFields().get(type.getFieldIndex("recordArrayField")).getType().getArrayElementType();
        Assert.assertEquals(4, typeChildren.getStructFields().size());
        Assert.assertTrue(typeChildren.getFieldIndex("stringField") >= 0);
        Assert.assertTrue(typeChildren.getFieldIndex("timestampField") >= 0);
        Assert.assertTrue(typeChildren.getFieldIndex("recordField") >= 0);
        Assert.assertTrue(typeChildren.getFieldIndex("recordArrayField") >= 0);

        final Type typeChildrenChild = typeChildren.getStructFields().get(typeChildren.getFieldIndex("recordField")).getType();
        Assert.assertEquals(2, typeChildrenChild.getStructFields().size());
        Assert.assertTrue(typeChildrenChild.getFieldIndex("intField") >= 0);
        Assert.assertTrue(typeChildrenChild.getFieldIndex("floatField") >= 0);

        Assert.assertEquals(Type.Code.ARRAY, typeChildren.getStructFields().get(typeChildren.getFieldIndex("recordArrayField")).getType().getCode());
        final Type typeChildrenChildren = typeChildren.getStructFields().get(typeChildren.getFieldIndex("recordArrayField")).getType().getArrayElementType();
        Assert.assertEquals(2, typeChildrenChildren.getStructFields().size());
        Assert.assertTrue(typeChildrenChildren.getFieldIndex("intField") >= 0);
        Assert.assertTrue(typeChildrenChildren.getFieldIndex("floatField") >= 0);

        // row test
        final Struct selectedStruct = StructSchemaUtil.toBuilder(type, struct).build();
        Assert.assertEquals(5, selectedStruct.getColumnCount());
        Assert.assertEquals(TestDatum.getStringFieldValue(), selectedStruct.getString("stringField"));
        Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), ((Long)selectedStruct.getLong("intField")).intValue());
        Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), selectedStruct.getLong("longField"));

        final Struct selectedStructChild = selectedStruct.getStruct("recordField");
        Assert.assertEquals(5, selectedStructChild.getColumnCount());
        Assert.assertEquals(TestDatum.getStringFieldValue(), selectedStructChild.getString("stringField"));
        Assert.assertEquals(TestDatum.getDoubleFieldValue().doubleValue(), selectedStructChild.getDouble("doubleField"), DELTA);
        Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedStructChild.getBoolean("booleanField"));

        final Struct selectedStructGrandchild = selectedStructChild.getStruct("recordField");
        Assert.assertEquals(2, selectedStructGrandchild.getColumnCount());
        Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(selectedStructGrandchild.getLong("intField")).intValue());
        Assert.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(selectedStructGrandchild.getDouble("floatField")).floatValue(), DELTA);

        Assert.assertEquals(2, selectedStruct.getStructList("recordArrayField").size());
        for(final Struct child : selectedStruct.getStructList("recordArrayField")) {
            Assert.assertEquals(4, child.getColumnCount());
            Assert.assertEquals(TestDatum.getStringFieldValue(), child.getString("stringField"));
            Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Timestamps.toMillis(child.getTimestamp("timestampField").toProto()));

            Assert.assertEquals(2, child.getStructList("recordArrayField").size());
            for(final Struct grandchild : child.getStructList("recordArrayField")) {
                Assert.assertEquals(2, grandchild.getColumnCount());
                Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(grandchild.getLong("intField")).intValue());
                Assert.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(grandchild.getDouble("floatField")).floatValue(), DELTA);
            }

            final Struct grandchild = child.getStruct("recordField");
            Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(grandchild.getLong("intField")).intValue());
            Assert.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(grandchild.getDouble("floatField")).floatValue(), DELTA);
        }

        // null fields row test
        final Struct structNull = TestDatum.generateStructNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Type typeNull = StructSchemaUtil.selectFields(structNull.getType(), newFields);

        final Struct selectedStructNull = StructSchemaUtil.toBuilder(typeNull, structNull).build();
        Assert.assertEquals(7, selectedStructNull.getColumnCount());
        Assert.assertTrue(selectedStructNull.isNull("stringField"));
        Assert.assertTrue(selectedStructNull.isNull("intField"));
        Assert.assertTrue(selectedStructNull.isNull("longField"));
        Assert.assertTrue(selectedStructNull.isNull("recordFieldNull"));
        Assert.assertTrue(selectedStructNull.isNull("recordArrayFieldNull"));

        final Struct selectedStructChildNull = selectedStructNull.getStruct("recordField");
        Assert.assertEquals(5, selectedStructChildNull.getColumnCount());
        Assert.assertTrue(selectedStructChildNull.isNull("stringField"));
        Assert.assertTrue(selectedStructChildNull.isNull("doubleField"));
        Assert.assertTrue(selectedStructChildNull.isNull("booleanField"));

        final Struct selectedStructGrandchildNull = selectedStructChildNull.getStruct("recordField");
        Assert.assertEquals(2, selectedStructGrandchildNull.getColumnCount());
        Assert.assertTrue(selectedStructGrandchildNull.isNull("intField"));
        Assert.assertTrue(selectedStructGrandchildNull.isNull("floatField"));

        Assert.assertEquals(2, selectedStructNull.getStructList("recordArrayField").size());
        for(final Struct child : selectedStructNull.getStructList("recordArrayField")) {
            Assert.assertEquals(4, child.getColumnCount());
            Assert.assertTrue(child.isNull("stringField"));
            Assert.assertTrue(child.isNull("timestampField"));

            Assert.assertEquals(2, child.getStructList("recordArrayField").size());
            for(final Struct grandchild : child.getStructList("recordArrayField")) {
                Assert.assertEquals(2, grandchild.getColumnCount());
                Assert.assertTrue(grandchild.isNull("intField"));
                Assert.assertTrue(grandchild.isNull("floatField"));
            }

            final Struct grandchild = child.getStruct("recordField");
            Assert.assertEquals(2, grandchild.getColumnCount());
            Assert.assertTrue(grandchild.isNull("intField"));
            Assert.assertTrue(grandchild.isNull("floatField"));
        }

    }

    @Test
    public void testFlattenType() {
        final Struct struct = createTestStruct();
        Type type = StructSchemaUtil.flatten(struct.getType(), "children", true);

        Set<String> fieldNames1 = type.getStructFields().stream()
                .map(Type.StructField::getName)
                .collect(Collectors.toSet());
        Assert.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildren","children_grandchildrenNull"),
                fieldNames1);
        struct.getType().getStructFields().forEach(f -> {
            if(f.getName().equals("stringField") || f.getName().equals("children_cstringField")) {
                Assert.assertEquals(
                        Type.Code.STRING,
                        f.getType().getCode());
            } else if(f.getName().equals("children_grandchild")) {
                Assert.assertEquals(
                        Type.Code.STRUCT,
                        f.getType().getCode());
            } else {
                Assert.assertEquals(
                        Type.Code.ARRAY,
                        f.getType().getCode());
                Assert.assertEquals(
                        Type.Code.STRUCT,
                        f.getType().getArrayElementType().getCode());
            }
        });

        final Type resultType2 = StructSchemaUtil.flatten(struct.getType(), "children.grandchildren", true);
        Set<String> fieldNames2 = resultType2.getStructFields().stream().map(Type.StructField::getName).collect(Collectors.toSet());
        Assert.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildren_gcstringField","children_grandchildrenNull"),
                fieldNames2);

        final Type resultType3 = StructSchemaUtil.flatten(struct.getType(), "children.grandchildrenNull", true);
        final Set<String> fieldNames3 = resultType3.getStructFields().stream()
                .map(Type.StructField::getName)
                .collect(Collectors.toSet());
        Assert.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildrenNull_gcstringField","children_grandchildren"),
                fieldNames3);

    }

    @Test
    public void testFlattenValues() {
        final Struct struct = createTestStruct();
        Type type = StructSchemaUtil.flatten(struct.getType(), "children", true);
        List<Struct> resultStructChildren1 = StructSchemaUtil.flatten(type, struct, "children", true);

        Assert.assertEquals(2, resultStructChildren1.size());

        // one path
        for(final Struct childrenStruct : resultStructChildren1) {
            Assert.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assert.assertEquals("cstringValue", childrenStruct.getString("children_cstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("children_grandchild");
            Assert.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));

            final List<Struct> grandchildrenStructs = childrenStruct.getStructList("children_grandchildren");
            Assert.assertEquals(2, grandchildrenStructs.size());
            for(final Struct grandchildrenStruct : grandchildrenStructs) {
                Assert.assertEquals("gcstringValue", grandchildrenStruct.getString("gcstringField"));
            }
        }

        // two path
        final Type resultTypeChildren2 = StructSchemaUtil.flatten(type, "children.grandchildren", true);
        final List<Struct> resultStructChildren2 = StructSchemaUtil.flatten(resultTypeChildren2, struct, "children.grandchildren", true);
        Assert.assertEquals(4, resultStructChildren2.size());

        for(final Struct childrenStruct : resultStructChildren2) {
            Assert.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assert.assertEquals("cstringValue", childrenStruct.getString("children_cstringField"));
            Assert.assertEquals("gcstringValue", childrenStruct.getString("children_grandchildren_gcstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("children_grandchild");
            Assert.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));
        }

        // one path without prefix
        final Type resultTypeChildren1WP = StructSchemaUtil.flatten(type, "children", false);
        final List<Struct> resultStructChildren1WP = StructSchemaUtil.flatten(resultTypeChildren1WP, struct, "children", false);
        Assert.assertEquals(2, resultStructChildren1WP.size());

        for(final Struct childrenStruct : resultStructChildren1WP) {
            Assert.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assert.assertEquals("cstringValue", childrenStruct.getString("cstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("grandchild");
            Assert.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));

            final Collection<Struct> grandchildrenStructs = childrenStruct.getStructList("grandchildren");
            Assert.assertEquals(2, grandchildrenStructs.size());
            for(final Struct grandchildrenStruct : grandchildrenStructs) {
                Assert.assertEquals("gcstringValue", grandchildrenStruct.getString("gcstringField"));
            }
        }

        // two path without prefix
        final Type resultTypeChildren2WP = StructSchemaUtil.flatten(type, "children.grandchildren", false);
        final List<Struct> resultStructChildren2WP = StructSchemaUtil.flatten(resultTypeChildren2WP, struct, "children.grandchildren", false);
        Assert.assertEquals(4, resultStructChildren2WP.size());

        for(final Struct childrenStruct : resultStructChildren2WP) {
            Assert.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assert.assertEquals("cstringValue", childrenStruct.getString("cstringField"));
            Assert.assertEquals("gcstringValue", childrenStruct.getString("gcstringField"));

            final Struct grandchildStruct = childrenStruct.getStruct("grandchild");
            Assert.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));
        }

        // Null check
        // two path null
        final Type resultTypeChildren2Null = StructSchemaUtil.flatten(type, "children.grandchildrenNull", true);
        final List<Struct> resultStructChildren2Null = StructSchemaUtil.flatten(resultTypeChildren2Null, struct, "children.grandchildrenNull", true);
        Assert.assertEquals(2, resultStructChildren2Null.size());
        System.out.println(resultStructChildren2Null.get(0).getType().getStructFields().stream().map(f -> f.getName()).collect(Collectors.toList()));

        for(final Struct childrenStruct : resultStructChildren2Null) {
            Assert.assertEquals("stringValue", childrenStruct.getString("stringField"));
            Assert.assertEquals("cstringValue", childrenStruct.getString("children_cstringField"));
        }

    }

    @Test
    public void testMerge() {
        final Struct struct = Struct.newBuilder()
                .set("str").to("a")
                .build();
        final Type childType = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("int", Type.string()),
                Type.StructField.of("float", Type.float64())
        );
        final Type type = Type.struct(
                Type.StructField.of("str", Type.string()),
                Type.StructField.of("int", Type.int64()),
                Type.StructField.of("float", Type.float64()),
                Type.StructField.of("struct", Type.struct(
                        Type.StructField.of("str", Type.string()),
                        Type.StructField.of("float", Type.float64())
                )),
                Type.StructField.of("structNo", Type.struct(
                        Type.StructField.of("str", Type.string()),
                        Type.StructField.of("float", Type.float64())
                )),
                Type.StructField.of("structNull", Type.struct(
                        Type.StructField.of("str", Type.string()),
                        Type.StructField.of("float", Type.float64())
                )),
                Type.StructField.of("boolArray", Type.array(Type.bool())),
                Type.StructField.of("dateArray", Type.array(Type.date())),
                Type.StructField.of("structArray", Type.array(childType))
        );

        final Map<String, Object> values = new HashMap<>();
        final Struct child = Struct.newBuilder().set("float").to(1D).build();
        final List<Struct> children = new ArrayList<>();
        children.add(Struct.newBuilder().set("str").to("b").build());
        values.put("struct", child);
        values.put("structNull", null);
        values.put("structArray", children);
        values.put("float", null);
        values.put("int", -1);
        values.put("boolArray", Arrays.asList(true, false, true));
        final Struct merged = StructSchemaUtil.merge(type, struct, values);

        Assert.assertEquals(9, merged.getType().getStructFields().size());
        Assert.assertEquals("a", merged.getString("str"));
        Assert.assertEquals(-1, merged.getLong("int"));
        Assert.assertEquals(Arrays.asList(true, false, true), merged.getBooleanList("boolArray"));
        Assert.assertTrue(merged.isNull("float"));
        Assert.assertTrue(merged.isNull("structNo"));
        Assert.assertTrue(merged.isNull("structNull"));
        Assert.assertTrue(merged.isNull("dateArray"));

        final Struct mergedChild = merged.getStruct("struct");
        Assert.assertEquals(2, mergedChild.getType().getStructFields().size());
        Assert.assertEquals(1D, mergedChild.getDouble("float"), DELTA);
        Assert.assertTrue(mergedChild.isNull("str"));

        Assert.assertEquals(1, merged.getStructList("structArray").size());
        for(final Struct c : merged.getStructList("structArray")) {
            Assert.assertEquals(3, c.getType().getStructFields().size());
            Assert.assertEquals("b", c.getString("str"));
            Assert.assertTrue(c.isNull("int"));
            Assert.assertTrue(c.isNull("float"));
        }
    }

    private Struct createTestStruct() {
        final Struct grandchild = Struct.newBuilder()
                .set("gcstringField").to("gcstringValue")
                .build();
        final Struct child = Struct.newBuilder()
                .set("cstringField").to("cstringValue")
                .set("grandchild").to(grandchild)
                .set("grandchildren").toStructArray(grandchild.getType(), Arrays.asList(grandchild, grandchild))
                .set("grandchildrenNull").toStructArray(grandchild.getType(),null)
                .build();
        final Struct struct = Struct.newBuilder()
                .set("stringField").to("stringValue")
                .set("children").toStructArray(child.getType(), Arrays.asList(child, child))
                //.set("childrenNull").to((Struct)null)
                .build();

        return struct;
    }

}
