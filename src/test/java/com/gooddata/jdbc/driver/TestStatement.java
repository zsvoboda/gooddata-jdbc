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


public class TestStatement {

    private final static Logger LOGGER = Logger.getLogger(TestStatement.class.getName());

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.Driver";
    private static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";

    private static final String COLUMNS = "\"Quarter/Year (Date)\", Product, Revenue::INTEGER, \"# of Orders::INTEGER\", \"Product Category\"";
    private static final String COLUMNS2 = "\"# of Orders\"";

    private Driver driver;
    private java.sql.Connection connection;
    private java.sql.Statement statement;
    private ResultSet resultSet;
    private ResultSet resultSet2;
    private List<String> columnnList;
    private List<String> columnnList2;

    public TestStatement() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);

        Parameters p = new Parameters();
        String url = String.format(DB_URL, p.getHost(), p.getWorkspace());
        this.driver = DriverManager.getDriver(url);
        this.connection = DriverManager.getConnection(url, p.getUsername(), p.getPassword());
        this.statement = (java.sql.Statement)this.connection.createStatement();
        this.resultSet = (ResultSet) this.statement.executeQuery("SELECT " + COLUMNS + " FROM DATA");
        this.resultSet2 = (ResultSet) this.statement.executeQuery("SELECT " + COLUMNS2 + " FROM DATA");
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
        };
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
        System.out.println(String.format("%s, %s, %s, %s, %s",
                this.resultSet.getString(1),
                this.resultSet.getString(2),
                this.resultSet.getFloat(3),
                this.resultSet.getInt(4),
                this.resultSet.getString(5)
                ));
        }
        this.resultSet.beforeFirst();
        System.out.println("\nRESULT 1 columns\n");
        while(this.resultSet.next()) {
            System.out.println(String.format("%s, %s, %s, %s, %s",
                    this.resultSet.getString(columnnList.get(0)),
                    this.resultSet.getString(columnnList.get(1)),
                    this.resultSet.getObject(columnnList.get(2)),
                    this.resultSet.getObject(columnnList.get(3)),
                    this.resultSet.getString(columnnList.get(4))
            ));
        }
    }

    @Test
    public void testGetValue2() throws SQLException {
        System.out.println("\nRESULT 2\n");
        while(this.resultSet2.next()) {
            System.out.println(String.format("%s",
                    this.resultSet2.getInt(1)
            ));
        }
        this.resultSet2.beforeFirst();
        System.out.println("\nRESULT 2 columns\n");
        while(this.resultSet2.next()) {
            System.out.println(String.format("%s",
                    this.resultSet2.getInt(columnnList2.get(0))
            ));
        }
    }


}
