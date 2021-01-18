package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.Parameters;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class TestAfmStatement {

    private final static Logger LOGGER = Logger.getLogger(TestAfmStatement.class.getName());

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.AfmDriver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";

    private static final String COLUMNS2 = "\"# of Orders\"";

    private final AfmConnection afmConnection;

    public TestAfmStatement() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.afmConnection = (AfmConnection) DriverManager.getConnection(url, p.getUsername(), p.getPassword());
    }

    public void testFindColumnIndex(String columns, String where, ResultSet resultSet) throws SQLException {
        final List<String> columnnList = Arrays.stream(columns.split(", "))
                .map(i->i.replaceAll("\"","")
                        .trim())
                .map(e->e.split("::")[0])
                .collect(Collectors.toList());
        int i = 1;
        for(String columnLabel: columnnList) {
            int columnIndex = resultSet.findColumn(columnLabel);
            assert(columnIndex == i);
            i++;
        }
    }

    public void testRetrieve(String columns, String where, String columnNames) throws SQLException {
        List<String> columnnList = Arrays.stream(
                (columnNames!=null && columnNames.trim().length()>0 ? columnNames : columns)
                .split(", "))
                    .map(i->i.replaceAll("\"","")
                            .trim())
                    .map(e->e.split("::")[0])
                    .collect(Collectors.toList());
        Statement statement = this.afmConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT " + columns + " " + where);

        testFindColumnIndex(columnNames != null && columnNames.trim().length()>0 ? columnNames : columns,
                where, resultSet);
        boolean printHeader = true;
        while(resultSet.next()) {
            if(printHeader) {
                String txtRow = "";
                for(int i=0; i< columnnList.size(); i++) {
                    if(i>0)
                        txtRow += ", ";
                    txtRow += resultSet.getMetaData().getColumnName(i+1);
                }
                System.out.println(txtRow);
                printHeader = false;
            }
            String txtRow = "";
            for(int i=0; i< columnnList.size(); i++) {
                if(i>0)
                    txtRow += ", ";
                txtRow += resultSet.getObject(i+1);
            }
            System.out.println(txtRow);
        }
        resultSet.beforeFirst();
        while(resultSet.next()) {
            String txtRow = "";
            for(int i=0; i< columnnList.size(); i++) {
                if(i>0)
                    txtRow += ", ";
                txtRow += resultSet.getObject(columnnList.get(i));
            }
        }
    }


    @Test
    public void testResultSet() throws SQLException {
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = 'Home' " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 ", null);
        testRetrieve("Revenue::INTEGER", " WHERE \"Product Category\" IN ('Home')",
                null);
        testRetrieve("\"[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/465]::INTEGER\"",
                " WHERE \"[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/272]\" IN ('Home')",
                "Revenue");
        testRetrieve("\"[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/272]\", \"[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/465]\"::INTEGER",
                " WHERE \"[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/272]\" IN ('Home')",
                "Product Category, Revenue");
    }

    @Test
    public void testCREATE() throws SQLException {
        Statement statement = this.afmConnection.createStatement();
        statement.execute("CREATE METRIC \"testNGMetric\" AS SELECT SUM(\"Revenue\") " +
                "WHERE \"Product Category\" IN ('Home','Electronics')");
        statement.execute("ALTER METRIC \"testNGMetric\" AS SELECT SUM(\"Revenue\") " +
                "WHERE \"Product Category\" IN ('Home')");
        statement.execute("DROP METRIC \"testNGMetric\";");
    }


}
