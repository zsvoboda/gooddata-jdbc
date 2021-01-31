package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.catalog.Catalog;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * GoodData JDBC driver
 */
public class AfmDriver implements java.sql.Driver {

    private final static Logger LOGGER = Logger.getLogger(AfmDriver.class.getName());

    public final static int MAJOR_VERSION = 0;
    public final static int MINOR_VERSION = 5;
    public final static String VERSION = String.format("%x.%x", MAJOR_VERSION, MINOR_VERSION);
    public static final String GDJDBC_DIR = String.format("%s/.gdjdbc",System.getProperty("user.home"));

    private static Driver registeredDriver;

    private static boolean isRegistered() {
        return registeredDriver != null;
    }

    /**
     * Setups the logging from the ~/.logging config file
     *
     * @throws IOException if the config file doesn't exist
     */
    static void setupLogging() throws IOException {
        Logger log = Logger.getGlobal();
        FileInputStream fis = new FileInputStream(String.format("%s/.logging",
                GDJDBC_DIR));
        LogManager.getLogManager().readConfiguration(fis);
        log.setUseParentHandlers(false);
        fis.close();
    }

    // Catalog cache
    private static final Map<String, Catalog> catalogs = new HashMap<>();

    public static Catalog getCatalog(String key) {
        return catalogs.get(key);
    }

    public static void cacheCatalog(String key, Catalog c) {
        catalogs.put(key, c);
    }

    static {
        try {
            setupLogging();
        }  catch (IOException e) {
            LOGGER.info("You can configure java.logging in the  ~/.logging file.");
        }
        try {
            if (isRegistered()) {
                throw new IllegalStateException("Driver is already registered. It can only be registered once.");
            } else {
                LOGGER.info("GooodData Workspace JDBC Driver started");
                AfmDriver registeredDriver = new AfmDriver();
                DriverManager.registerDriver(registeredDriver);
                AfmDriver.registeredDriver = registeredDriver;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Default constructor
     */
    public AfmDriver() {
        LOGGER.info("AfmDriver");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        LOGGER.info(String.format("connect: url='%s', info = '%s'", url, info));
        if (this.acceptsURL(url)) {
            try {
                return new AfmConnection(url, info);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else {
            throw new SQLException("Invalid JDBC URL. The driver URL must follow this format" +
                    " 'jdbc:gd://<gooddata-server>/gdc/projects/<workspace-id>'.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptsURL(String url) {
        LOGGER.info(String.format("acceptsURL: url='%s'", url));
        boolean b = url.startsWith("jdbc:gd:");
        LOGGER.info(String.format("acceptsURL: returning='%b'", b));
        return b;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        LOGGER.info(String.format("getPropertyInfo: url='%s' info='%s'", url, info));
        return new DriverPropertyInfo[]{};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMajorVersion() {
        LOGGER.info("getMajorVersion");
        return MAJOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMinorVersion() {
        LOGGER.info("getMinorVersion");
        return MINOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean jdbcCompliant() {
        LOGGER.info("jdbcCompliant");
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Logger getParentLogger() {
        LOGGER.info("getParentLogger");
        return LOGGER;
    }

}
