package com.gooddata.jdbc.util;

import com.gooddata.jdbc.Parameters;
import com.gooddata.jdbc.driver.DatabaseMetaData;
import org.testng.annotations.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

public class TestTextUtil {

    @Test
    public void testExtractPid() throws SQLException {
        assert(
                TextUtil.extractWorkspaceIdFromUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386").
                        equals("w2x7a9awsioch4l9lbzgjcn99hbkm61e"));
        assert(
                !TextUtil.extractWorkspaceIdFromUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386").
                        equals("obj"));
    }

    @Test(expectedExceptions = { SQLException.class })
    public void testParseBoolException() throws SQLException {
        TextUtil.extractWorkspaceIdFromUri("/gdc/obj/w2x7a9awsioch4l9lbzgjcn99hbkm61e/s386");
    }

}
