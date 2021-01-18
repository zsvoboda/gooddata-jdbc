package com.gooddata.jdbc.catalog;

import com.gooddata.jdbc.driver.AfmDriver;
import com.gooddata.jdbc.util.Parameters;
import com.gooddata.sdk.service.GoodData;
import org.testng.annotations.Test;

import java.util.List;

public class TestSchema {


    @Test
    public void testAllSchemas() {
        Parameters p = new Parameters();
        GoodData gd = new GoodData(p.getHost(), p.getUsername(), p.getPassword());
        List<Schema> schemas = Schema.populateSchemas(gd);
        System.out.println(schemas);
    }

}
