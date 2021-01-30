package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.Parameters;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class TestAfmStatement {

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.AfmDriver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";
    private final AfmConnection afmConnection;

    public TestAfmStatement() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.afmConnection = (AfmConnection) DriverManager.getConnection(url, p.getUsername(), p.getPassword());
    }

    public void testFindColumnIndex(String columns, ResultSet resultSet) throws SQLException {
        final List<String> columnnList = Arrays.stream(columns.split(", "))
                .map(i -> i.replaceAll("\"", "")
                        .trim())
                .map(e -> e.split("::")[0])
                .collect(Collectors.toList());
        int i = 1;
        for (String columnLabel : columnnList) {
            int columnIndex = resultSet.findColumn(columnLabel);
            assert (columnIndex == i);
            i++;
        }
    }

    public void testRetrieve(String columns, String where, String columnNames) throws SQLException {
        String s = columnNames != null && columnNames.trim().length() > 0 ? columnNames : columns;
        List<String> columnnList = Arrays.stream(
                s.split(", ")).map(i -> i.replaceAll("\"", "")
                .trim())
                .map(e -> e.split("::")[0])
                .collect(Collectors.toList());
        Statement statement = this.afmConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT " + columns + " " + where);

        testFindColumnIndex(s, resultSet);
        StringBuilder txtRow = new StringBuilder();
        for (int i = 0; i < columnnList.size(); i++) {
            if (i > 0)
                txtRow.append(", ");
            txtRow.append(resultSet.getMetaData().getColumnName(i + 1));
        }
        System.out.println(txtRow.toString());

        int rowNum = 1;
        while (resultSet.next()) {
            txtRow = new StringBuilder();
            for (int i = 0; i < columnnList.size(); i++) {
                if (i > 0)
                    txtRow.append(", ");
                else {
                    txtRow.append(rowNum++);
                    txtRow.append("::");
                }
                txtRow.append(resultSet.getObject(columnnList.get(i)));
            }
            System.out.println(txtRow.toString());
        }
    }

    public void testRetreivePreparedStatement(String columns, String where, String columnNames, Object[] values) throws SQLException {
        String s = columnNames != null && columnNames.trim().length() > 0 ? columnNames : columns;
        List<String> columnnList = Arrays.stream(
                s.split(", ")).map(i -> i.replaceAll("\"", "")
                .trim())
                .map(e -> e.split("::")[0])
                .collect(Collectors.toList());
        AfmStatement statement = (AfmStatement) this.afmConnection.prepareStatement("SELECT " + columns + " " + where);
        for(int i=0; i< values.length; i++) {
            statement.setObject(i+1, values[i]);
        }
        ResultSet resultSet = statement.executeQuery();


        testFindColumnIndex(s, resultSet);
        StringBuilder txtRow = new StringBuilder();
        for (int i = 0; i < columnnList.size(); i++) {
            if (i > 0)
                txtRow.append(", ");
            txtRow.append(resultSet.getMetaData().getColumnName(i + 1));
        }
        System.out.println(txtRow.toString());

        int rowNum = 1;
        while (resultSet.next()) {
            txtRow = new StringBuilder();
            for (int i = 0; i < columnnList.size(); i++) {
                if (i > 0)
                    txtRow.append(", ");
                else {
                    txtRow.append(rowNum++);
                    txtRow.append("::");
                }
                txtRow.append(resultSet.getObject(columnnList.get(i)));
            }
            System.out.println(txtRow.toString());
        }
    }


    @Test
    public void testOrderBy() throws SQLException {
        testRetrieve("\"Product Category\", \"Product\", \"# of Orders\"",
                "ORDER BY \"Product\"", null);
    }

    @Test
    public void testPrecision() {
        //testRetrieve("\"PRODUCT_NAME\", \"ORDER_AMOUNT_METRIC\"","", null);
    }

    @Test
    public void testResultSet() throws SQLException {
        testRetrieve("\"Date (Date)\", \"Product Category\", \"Product\", \"# of Orders\"",
                "LIMIT 5000 OFFSET 2", null);
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = 'Home' " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 OFFSET 6", null);
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = 'Home' " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 OFFSET 6 LIMIT 1", null);
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = 'Home' " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 OFFSET 6 LIMIT 2", null);
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = 'Home' " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 OFFSET 7", null);
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = 'Home' " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 OFFSET 8", null);
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
    public void testPreparedStatement() throws SQLException {
        testRetreivePreparedStatement("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = ? " +
                "AND \"# of Orders\" BETWEEN ? AND ? OFFSET 6 LIMIT 1", null, new Object[]{"Home", 3, 5});
    }

    @Test(expectedExceptions = { SQLException.class })
    public void testErrors() throws SQLException {
        testRetreivePreparedStatement("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = ? " +
                "AND \"# of Orders\" BETWEEN ? AND ? OFFSET 6 LIMIT 1", null, new Object[]{"Home", 3});
        testRetrieve("\"Date (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\"," +
                " \"Product Category\"", " WHERE \"Product Category\" = Home " +
                "AND \"# of Orders\" BETWEEN 3 AND 5 OFFSET 6", null);
    }


    @Test
    public void testCreateAlterDescribeDrop() throws SQLException {
        Statement statement = this.afmConnection.createStatement();
        statement.execute("CREATE METRIC \"testNGMetric\" AS SELECT SUM(\"Revenue\") " +
                "BY \"Year (Date)\" ALL OTHER");
        statement.execute("DESCRIBE METRIC \"testNGMetric\";");
        statement.execute("ALTER METRIC \"testNGMetric\" AS SELECT SUM(\"Revenue\") " +
                "WHERE \"Product Category\" IN ('Home')");
        statement.execute("DESCRIBE METRIC \"testNGMetric\";");
        statement.execute("DROP METRIC \"testNGMetric\";");
    }


}
