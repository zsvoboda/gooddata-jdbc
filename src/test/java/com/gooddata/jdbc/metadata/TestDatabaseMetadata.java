package com.gooddata.jdbc.metadata;

import com.gooddata.jdbc.util.Parameters;
import com.gooddata.jdbc.catalog.Catalog;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class TestDatabaseMetadata {

    private final static Logger LOGGER = Logger.getLogger(TestDatabaseMetadata.class.getName());

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.AfmDriver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";

    private final java.sql.Connection connection;

    public TestDatabaseMetadata() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.connection = DriverManager.getConnection(url, p.getUsername(), p.getPassword());
    }

    private void printResultSet(ResultSet r) throws SQLException {
        int colCnt = r.getMetaData().getColumnCount();
        StringBuffer header = new StringBuffer();
        for(int i=1; i<= colCnt; i++) {
            header.append(r.getMetaData().getColumnName(i));
            if(i<colCnt)
                header.append(", ");
        }
        System.out.println(header);
        while(r.next()) {
            StringBuffer row = new StringBuffer();
            for(int i=1; i<=colCnt; i++) {
                row.append(r.getObject(i));
                if(i<colCnt)
                    row.append(", ");
            }
            System.out.println(row);
        }
        r.close();
    }

    @Test
    public void testFindColumnIndex() throws SQLException {

    }

    @Test
    public void testAttributeElementsLookup() throws Catalog.CatalogEntryNotFoundException, SQLException {
        AfmDatabaseMetaData dm = (AfmDatabaseMetaData)this.connection.getMetaData();
        List<String> values = Arrays.asList("Home","Electronics");
        Map<String,String> m = dm.getGoodDataRestConnection().lookupAttributeElements(
                "/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/272",
                values);
        for(String value: values) {
            assert(m.containsKey(value));
        }
        m = dm.getGoodDataRestConnection().lookupAttributeElements(
                "/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/272",
                Arrays.asList("NONEXISTENT1","NONEXISTENT2"));
        m = dm.getGoodDataRestConnection().lookupAttributeElements(
                "/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/272",
                Arrays.asList("NONEXISTENT1"));
    }


}
