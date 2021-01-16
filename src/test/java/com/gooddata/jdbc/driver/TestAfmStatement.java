package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.Parameters;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class TestAfmStatement {

    private final static Logger LOGGER = Logger.getLogger(TestAfmStatement.class.getName());

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.AfmDriver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";

    private static final String COLUMNS = "\"Quarter/Year (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\", \"Product Category\"";
    private static final String COLUMNS2 = "\"# of Orders\"";

    private final AfmConnection afmConnection;
    private final java.sql.Statement statement;
    private final ResultSet resultSet;
    private final ResultSet resultSet2;
    private final List<String> columnnList;
    private final List<String> columnnList2;

    public TestAfmStatement() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);

        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.afmConnection = (AfmConnection) DriverManager.getConnection(url, p.getUsername(), p.getPassword());
        this.statement = afmConnection.createStatement();
        this.resultSet = this.statement.executeQuery("SELECT " + COLUMNS +
                " WHERE \"Product Category\" = 'Home' AND REVENUE > 1000 AND \"# of Orders\" > 20");
        this.resultSet2 = this.statement.executeQuery("SELECT " + COLUMNS2);
        this.columnnList = Arrays.stream(COLUMNS.split(", "))
                .map(i->i.replaceAll("\"","")
                .trim())
                .map(e->e.split("::")[0])
                .collect(Collectors.toList());
        this.columnnList2 = Arrays.stream(COLUMNS2.split(", "))
                .map(i->i.replaceAll("\"","").trim())
                .map(e->e.split("::")[0])
                .collect(Collectors.toList());

    }

    @Test
    public void testFindColumnIndex() throws SQLException {
        int i = 1;
        for(String columnLabel: this.columnnList) {
            int columnIndex = this.resultSet.findColumn(columnLabel);
            assert(columnIndex == i);
            i++;
        }
        i = 1;
        for(String columnLabel: this.columnnList2) {
            int columnIndex = this.resultSet2.findColumn(columnLabel);
            assert(columnIndex == i);
            i++;
        }
    }


    @Test
    public void testGetValue() throws SQLException {
        System.out.println("\nRESULT 1\n");
        while(this.resultSet.next()) {
        System.out.printf("%s, %s, %s, %s, %s%n",
                this.resultSet.getString(1),
                this.resultSet.getString(2),
                this.resultSet.getFloat(3),
                this.resultSet.getInt(4),
                this.resultSet.getString(5)
                );
        }
        this.resultSet.beforeFirst();
        System.out.println("\nRESULT 1 columns\n");
        while(this.resultSet.next()) {
            System.out.printf("%s, %s, %s, %s, %s%n",
                    this.resultSet.getString(columnnList.get(0)),
                    this.resultSet.getString(columnnList.get(1)),
                    this.resultSet.getObject(columnnList.get(2)),
                    this.resultSet.getObject(columnnList.get(3)),
                    this.resultSet.getString(columnnList.get(4))
            );
        }
    }

    @Test
    public void testGetValue2() throws SQLException {
        System.out.println("\nRESULT 2\n");
        while(this.resultSet2.next()) {
            System.out.printf("%s%n",
                    this.resultSet2.getInt(1)
            );
        }
        this.resultSet2.beforeFirst();
        System.out.println("\nRESULT 2 columns\n");
        while(this.resultSet2.next()) {
            System.out.printf("%s%n",
                    this.resultSet2.getInt(columnnList2.get(0))
            );
        }
    }

    @Test
    public void testCREATE() throws SQLException {

        this.statement.execute("CREATE METRIC \"testNGMetric\" AS SELECT SUM(\"Revenue\") " +
                "WHERE \"Product Category\" IN ('Home','Electronics')");
        this.statement.execute("ALTER METRIC \"testNGMetric\" AS SELECT SUM(\"Revenue\") " +
                "WHERE \"Product Category\" IN ('Home')");
        this.statement.execute("DROP METRIC \"testNGMetric\";");
    }

}
