package com.gooddata.jdbc.util;

import com.gooddata.jdbc.Parameters;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.md.MetadataService;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class TestLdmObjects {

    @Test
    public void testCreateMetric() throws SQLException {

        Parameters p = new Parameters();
        GoodData gd = new GoodData(p.getHost(), p.getUsername(), p.getPassword());

        Project workspace = gd.getProjectService().getProjectById(p.getWorkspace());
        MetadataService md = gd.getMetadataService();

        Metric m = new Metric("test",
                "SELECT SUM([/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/274])",
                "###");
        //md.createObj(workspace, m);

    }

}
