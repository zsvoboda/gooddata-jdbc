package com.gooddata.jdbc.resultset;

import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TestMetadataResultSet {

    private final MetadataResultSet mrs;

    public TestMetadataResultSet() {
        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                        Arrays.asList("1","2")),
                new MetadataResultSet.MetaDataColumn("TABLE_CATALOG",
                        Arrays.asList("A","B"))
        );
        this.mrs = new MetadataResultSet(data);
    }

    @Test
    public void testData() throws SQLException {
        assert(this.mrs.next());
        assert(this.mrs.getString(1).equals("1"));
        assert(this.mrs.getString("TABLE_CATALOG").equals("A"));
        assert(this.mrs.next());
        assert(this.mrs.getString("TABLE_SCHEM").equals("2"));
        assert(this.mrs.getString(2).equals("B"));
        assert(!this.mrs.next());
    }

}
