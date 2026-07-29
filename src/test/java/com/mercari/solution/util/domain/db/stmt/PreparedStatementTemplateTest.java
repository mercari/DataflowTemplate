package com.mercari.solution.util.domain.db.stmt;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class PreparedStatementTemplateTest {

    private static int[] toIntArray(List<Integer> list) {
        return list.stream().mapToInt(x->x).toArray();
    }

    private static Object defaultValue(final Class<?> returnType) {
        if(returnType.equals(Void.TYPE)) {
            return null;
        } else if(returnType.equals(Boolean.TYPE)) {
            return false;
        } else if(returnType.equals(Byte.TYPE)) {
            return (byte) 0;
        } else if(returnType.equals(Short.TYPE)) {
            return (short) 0;
        } else if(returnType.equals(Integer.TYPE)) {
            return 0;
        } else if(returnType.equals(Long.TYPE)) {
            return 0L;
        } else if(returnType.equals(Float.TYPE)) {
            return 0F;
        } else if(returnType.equals(Double.TYPE)) {
            return 0D;
        } else if(returnType.equals(Character.TYPE)) {
            return '\0';
        } else {
            return null;
        }
    }

    @Test
    public void testSerializable() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ").appendPlaceholder(1);

        final PreparedStatementTemplate original = builder.build();
        final PreparedStatementTemplate copied = SerializationUtils.clone(original);

        Assert.assertEquals(original, copied);
    }

    @Test
    public void testPlaceholdersUsedOnce() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ")
                .appendPlaceholder(1)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(3);

        final PreparedStatementTemplate template = builder.build();
        final List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        Assert.assertEquals("SELECT ?, ?, ?", template.getStatementString());

        Assert.assertEquals(3 + 1, mappings.size());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(3)));
    }

    @Test
    public void testPlaceholdersUsedMultipleTimes() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ")
                .appendPlaceholder(1)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(3)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(3)
                .appendString(", ")
                .appendPlaceholder(3);

        final PreparedStatementTemplate template = builder.build();
        final List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        Assert.assertEquals("SELECT ?, ?, ?, ?, ?, ?", template.getStatementString());

        Assert.assertEquals(3 + 1, mappings.size());
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{2, 4}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{3, 5, 6}, toIntArray(mappings.get(3)));
    }

    @Test
    public void testPlaceholdersUsedShuffledOrder() {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("SELECT ")
                .appendPlaceholder(3)
                .appendString(", ")
                .appendPlaceholder(1)
                .appendString(", ")
                .appendPlaceholder(2)
                .appendString(", ")
                .appendPlaceholder(4);

        final PreparedStatementTemplate template = builder.build();
        final List<List<Integer>> mappings = template.getPlaceholderMappings().getMappings();

        Assert.assertEquals("SELECT ?, ?, ?, ?", template.getStatementString());

        Assert.assertEquals(4 + 1, mappings.size());
        Assert.assertArrayEquals(new int[]{2}, toIntArray(mappings.get(1)));
        Assert.assertArrayEquals(new int[]{3}, toIntArray(mappings.get(2)));
        Assert.assertArrayEquals(new int[]{1}, toIntArray(mappings.get(3)));
        Assert.assertArrayEquals(new int[]{4}, toIntArray(mappings.get(4)));
    }

    @Test
    public void testPlaceholderSetterProxyWithOffset() throws Exception {
        final PreparedStatementTemplate.Builder builder = new PreparedStatementTemplate.Builder();
        builder.appendString("INSERT INTO people VALUES (")
                .appendPlaceholder(1)
                .appendString(",")
                .appendPlaceholder(2)
                .appendString("),(")
                .appendPlaceholder(3)
                .appendString(",")
                .appendPlaceholder(4)
                .appendString(")");

        final PreparedStatementTemplate template = builder.build();
        final List<String> calls = new ArrayList<>();
        final PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if(method.getName().equals("setString") || method.getName().equals("setInt")) {
                        calls.add(method.getName() + ":" + args[0] + ":" + args[1]);
                    }
                    return defaultValue(method.getReturnType());
                });

        template.createPlaceholderSetterProxy(statement, 2).setString(1, "alice");
        template.createPlaceholderSetterProxy(statement, 2).setInt(2, 20);

        Assert.assertEquals(Arrays.asList("setString:3:alice", "setInt:4:20"), calls);
    }
}
