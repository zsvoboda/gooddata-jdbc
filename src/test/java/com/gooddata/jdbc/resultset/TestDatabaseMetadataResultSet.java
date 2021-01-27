package com.gooddata.jdbc.resultset;

import com.gooddata.jdbc.driver.AfmConnection;
import com.gooddata.jdbc.util.Parameters;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestDatabaseMetadataResultSet {

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.AfmDriver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";
    private final AfmConnection afmConnection;

    public TestDatabaseMetadataResultSet() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.afmConnection = (AfmConnection) DriverManager.getConnection(url, p.getUsername(), p.getPassword());
    }

    public void printResultSet(ResultSet rs) throws SQLException {

        StringBuilder txtRow = new StringBuilder();
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            if (i > 0)
                txtRow.append(", ");
            txtRow.append(rs.getMetaData().getColumnName(i + 1));
        }
        System.out.println(txtRow.toString());

        int rowNum = 1;
        while (rs.next()) {
            txtRow = new StringBuilder();
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                if (i > 0)
                    txtRow.append(", ");
                else {
                    txtRow.append(rowNum++);
                    txtRow.append("::");
                }
                txtRow.append(rs.getObject(i + 1));
            }
            System.out.println(txtRow.toString());
        }
    }


    @Test
    public void testColumnsResultSet() throws SQLException {
        ResultSet r = this.afmConnection.getMetaData().getColumns("",
                "","", "");
        printResultSet(r);

    }



}
