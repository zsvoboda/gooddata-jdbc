package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.Parameters;
import org.testng.annotations.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class TestDatabaseMetadata {

    private final static Logger LOGGER = Logger.getLogger(TestDatabaseMetadata.class.getName());

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.Driver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";

    private Driver driver;
    private java.sql.Connection connection;

    public TestDatabaseMetadata() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.driver = DriverManager.getDriver(url);
        this.connection = DriverManager.getConnection(url, p.getUsername(), p.getPassword());
    }

    private void printResultSet(ResultSet r) throws SQLException {
        int colCnt = r.getMetaData().getColumnCount();
        StringBuffer header = new StringBuffer("");
        for(int i=1; i<= colCnt; i++) {
            header.append(r.getMetaData().getColumnName(i));
            if(i<colCnt)
                header.append(", ");
        }
        System.out.println(header);
        while(r.next()) {
            StringBuffer row = new StringBuffer("");
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
        java.sql.DatabaseMetaData dm = this.connection.getMetaData();
        System.out.println("\nTABLE TYPES\n");
        printResultSet(dm.getTableTypes());
        System.out.println("\nCATALOGS\n");
        printResultSet(dm.getCatalogs());
        System.out.println("\nSCHEMAS\n");
        printResultSet(dm.getSchemas());
        System.out.println("\nTABLES\n");
        printResultSet(dm.getTables("","","", null));
        System.out.println("\nCOLUMNS\n");
        printResultSet(dm.getColumns("","","",""));
    }


}
