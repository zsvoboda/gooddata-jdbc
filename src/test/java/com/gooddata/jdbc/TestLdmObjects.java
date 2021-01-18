package com.gooddata.jdbc;

import com.gooddata.jdbc.util.Parameters;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.service.GoodData;
import org.testng.annotations.Test;

public class TestLdmObjects {

    @Test
    public void testCreateMetric() {

        Parameters p = new Parameters();
        GoodData gd = new GoodData(p.getHost(), p.getUsername(), p.getPassword());
        Metric m = new Metric("test",
                "SELECT SUM([/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/274])",
                "###");
        //md.createObj(workspace, m);

    }

}
