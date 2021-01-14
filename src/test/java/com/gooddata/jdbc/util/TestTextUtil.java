package com.gooddata.jdbc.util;

import org.testng.annotations.Test;

import java.sql.SQLException;

public class TestTextUtil {

    @Test
    public void testExtractPid() throws SQLException {
        assert(
                TextUtil.extractIdFromUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386").
                        equals("w2x7a9awsioch4l9lbzgjcn99hbkm61e"));
        assert(
                !TextUtil.extractIdFromUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386").
                        equals("obj"));
    }

    @Test(expectedExceptions = { SQLException.class })
    public void testParseBoolException() throws SQLException {
        TextUtil.extractIdFromUri("/gdc/obj/w2x7a9awsioch4l9lbzgjcn99hbkm61e/s386");
    }

}
