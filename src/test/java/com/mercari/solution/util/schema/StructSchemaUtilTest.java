package com.mercari.solution.util.schema;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StructSchemaUtilTest {

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
