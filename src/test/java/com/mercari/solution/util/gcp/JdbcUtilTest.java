package com.mercari.solution.util.gcp;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JdbcUtilTest {

    @Test
    public void testCreateSeekConditions() {

        // Test start conditions
        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        final List<String> conditions1 = JdbcUtil.createSeekConditions(startOffsets1, true,true);
        final String result1 = "(" + String.join(" OR ", conditions1) + ")";
        Assert.assertEquals("(intField1 > ?)", result1);

        final List<JdbcUtil.IndexOffset> startOffsets2 = new ArrayList<>();
        startOffsets2.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets2.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        final List<String> conditions2 = JdbcUtil.createSeekConditions(startOffsets2, true, true);
        final String result2 = "(" + String.join(" OR ", conditions2) + ")";
        Assert.assertEquals("((intField1 = ? AND intField2 > ?) OR intField1 > ?)", result2);

        final List<JdbcUtil.IndexOffset> startOffsets3 = new ArrayList<>();
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField3", Schema.Type.INT, true, 1));
        final List<String> conditions3 = JdbcUtil.createSeekConditions(startOffsets3, true, true);
        final String result3 = "(" + String.join(" OR ", conditions3) + ")";
        Assert.assertEquals("((intField1 = ? AND intField2 = ? AND intField3 > ?) OR (intField1 = ? AND intField2 > ?) OR intField1 > ?)", result3);

        // Test stop conditions
        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        final List<String> stopConditions1 = JdbcUtil.createSeekConditions(stopOffsets1, false, false);
        final String stopResult1 = "(" + String.join(" OR ", stopConditions1) + ")";
        Assert.assertEquals("(intField1 <= ?)", stopResult1);

        final List<JdbcUtil.IndexOffset> stopOffsets2 = new ArrayList<>();
        stopOffsets2.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        stopOffsets2.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        final List<String> stopConditions2 = JdbcUtil.createSeekConditions(stopOffsets2, false, false);
        final String stopResult2 = "(" + String.join(" OR ", stopConditions2) + ")";
        Assert.assertEquals("((intField1 = ? AND intField2 <= ?) OR intField1 <= ?)", stopResult2);
    }

    @Test
    public void testCreateSeekPreparedQuery() {

        final List<JdbcUtil.IndexOffset> startOffsets3 = new ArrayList<>();
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 5));
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        startOffsets3.add(JdbcUtil.IndexOffset.of("intField3", Schema.Type.INT, true, 1));

        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 7));

        final String table = "mytable";
        final String fields = "intField1,intField2,intField3";
        final List<String> parameterFields = Arrays.asList("intField1", "intField2", "intField3");
        final String preparedQuery1 = JdbcUtil.createSeekPreparedQuery(JdbcUtil.IndexPosition.of(startOffsets3, true), JdbcUtil.IndexPosition.of(stopOffsets1, false), fields, table, parameterFields, 1000);
        Assert.assertEquals("SELECT intField1,intField2,intField3 FROM mytable WHERE ((intField1 = ? AND intField2 = ? AND intField3 > ?) OR (intField1 = ? AND intField2 > ?) OR intField1 > ?) AND (intField1 <= ?) ORDER BY intField1, intField2, intField3 LIMIT 1000", preparedQuery1);
    }

    @Test
    public void testSplitIndexRange() {
        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 1));
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 3));
        startOffsets1.add(JdbcUtil.IndexOffset.of("intField3", Schema.Type.INT, true, 1));

        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField1", Schema.Type.INT, true, 30));
        stopOffsets1.add(JdbcUtil.IndexOffset.of("intField2", Schema.Type.INT, true, 70));

        final JdbcUtil.IndexPosition start = JdbcUtil.IndexPosition.of(startOffsets1, true);
        final JdbcUtil.IndexPosition stop = JdbcUtil.IndexPosition.of(stopOffsets1, false);
        final JdbcUtil.IndexRange originalRange = JdbcUtil.IndexRange.of(start, stop);
        final List<JdbcUtil.IndexRange> splittedRanges1 = JdbcUtil
                .splitIndexRange(
                        null,
                        originalRange.getFrom().getOffsets(),
                        originalRange.getTo().getOffsets(),
                        4);
        for(JdbcUtil.IndexRange range : splittedRanges1) {
            // TODO
        }
    }

    @Test
    public void testSplitIndexRangeString() {
        final List<JdbcUtil.IndexOffset> startOffsets1 = new ArrayList<>();
        startOffsets1.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, ""));
        startOffsets1.add(JdbcUtil.IndexOffset.of("strField2", Schema.Type.STRING, true, "add"));
        startOffsets1.add(JdbcUtil.IndexOffset.of("strField3", Schema.Type.STRING, true, "aff"));

        final List<JdbcUtil.IndexOffset> stopOffsets1 = new ArrayList<>();
        stopOffsets1.add(JdbcUtil.IndexOffset.of("strField1", Schema.Type.STRING, true, "znbff"));
        stopOffsets1.add(JdbcUtil.IndexOffset.of("strField2", Schema.Type.STRING, true, "zdf"));

        final JdbcUtil.IndexPosition start = JdbcUtil.IndexPosition.of(startOffsets1, true);
        final JdbcUtil.IndexPosition stop = JdbcUtil.IndexPosition.of(stopOffsets1, false);
        final JdbcUtil.IndexRange originalRange = JdbcUtil.IndexRange.of(start, stop);
        final List<JdbcUtil.IndexRange> splittedRanges1 = JdbcUtil
                .splitIndexRange(
                        null,
                        originalRange.getFrom().getOffsets(),
                        originalRange.getTo().getOffsets(),
                        4);
        for(JdbcUtil.IndexRange range : splittedRanges1) {
            // TODO
        }
    }

}
