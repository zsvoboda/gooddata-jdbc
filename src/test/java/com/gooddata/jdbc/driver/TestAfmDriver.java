package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.Parameters;
import org.testng.annotations.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public class TestAfmDriver {

    private final static Logger LOGGER = Logger.getLogger(TestAfmDriver.class.getName());

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.gooddata.jdbc.driver.AfmDriver";
    static final String DB_URL = "jdbc:gd://%s/gdc/projects/%s";

    private final Driver driver;

    public TestAfmDriver() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        Parameters p = new Parameters();
        this.driver = DriverManager.getDriver(String.format(DB_URL, p.getHost(), p.getWorkspace()));
    }

    @Test
    public void testVersion() {
        assert ("0.5".equals(AfmDriver.VERSION));
        assert (this.driver.getMajorVersion() == 0);
        assert (this.driver.getMinorVersion() == 5);
    }

    @Test
    public void testJdbcCompliant() {
        assert (!this.driver.jdbcCompliant());
    }

    @Test
    public void testAcceptsUrl() throws SQLException {
        assert (this.driver.acceptsURL("jdbc:gd://secure.na.gooddata.com/gdc/projects/la84vcyhrq8jwbu4wpipw66q2sqeb923"));
        assert (!this.driver.acceptsURL("jdbc:maql://secure.na.gooddata.com/gdc/projects/la84vcyhrq8jwbu4wpipw66q2sqeb923"));
    }

}
