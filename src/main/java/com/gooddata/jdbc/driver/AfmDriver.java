package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.catalog.Catalog;

import java.io.FileInputStream;
import java.io.IOException;
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

    /**
     * Setups the logging from the ~/.logging config file
     *
     * @throws IOException if the config file doesn't exist
     */
    static void setupLogging() throws IOException {
        Logger log = Logger.getGlobal();
        FileInputStream fis = new FileInputStream(String.format("%s/.logging",
                System.getProperty("user.home")));
        //FileInputStream fis = new FileInputStream("/.logging");
        LogManager.getLogManager().readConfiguration(fis);
        log.setUseParentHandlers(false);
        fis.close();
    }

    static {
        try {
            setupLogging();
        }  catch (IOException e) {
            LOGGER.info("You can configure java.logging in the  ~/.logging file.");
        }
        try {
            LOGGER.info("GooodData Workspace JDBC Driver started");
            DriverManager.registerDriver(new AfmDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Caching catalogs by schema
    private static final Map<String, Catalog> catalogCache = new HashMap<>();

    /**
     * Default constructor
     */
    public AfmDriver() {
        LOGGER.info("AfmDriver");
    }

	/**
	 * Returns cached catalog or NULL
	 * @param schema schema key
	 * @return cached catalog
	 */
	public static Catalog getCachedCatalog(String schema) {
        LOGGER.info(String.format("getCachedCatalog: schema='%s'", schema));
	    return catalogCache.get(schema);
	}

	/**
	 * Cache catalog
	 * @param schema schema key
	 * @param catalog catalog to cache
	 */
	public static void cacheCatalog(String schema, Catalog catalog) {
        LOGGER.info(String.format("cacheCatalog: schema='%s'", schema));
	    catalogCache.put(schema, catalog);
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
        return url.startsWith("jdbc:gd:");
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
