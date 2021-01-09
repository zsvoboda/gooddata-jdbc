package com.gooddata.jdbc.driver;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {

	private final static Logger logger = Logger.getGlobal();

	private final static int MAJOR_VERSION = 0;
	private final static int MINOR_VERSION = 5;
	public final static String VERSION = String.format("%x.%x", MAJOR_VERSION, MINOR_VERSION);

	 static {
	        try {
	        	logger.info("GooodData JDBC Driver started");
	            DriverManager.registerDriver(new Driver());
	        } catch (SQLException e) {
	            throw new RuntimeException(e);
	        }
	    }

	/**
	 * Default constructor
	 */
    private Driver() {}

	/**
	 * Creates a new connection
	 * @param url - JDBC URL
	 * @param info - JDBC properties
	 * @return new Connection
	 * @throws SQLException in case of an issue
	 */
	@Override
	public java.sql.Connection connect(String url, Properties info) throws SQLException {
		if (this.acceptsURL(url)) {
        	if (info.getProperty("debug", "false").equals("true"))
        	{
        	    logger.setLevel(Level.INFO);
        	}
        	else
        	{
        		logger.setLevel(Level.WARNING);
        	}
        	logger.info("jdbc4gd: connect url:"+url);
			try {
				return new Connection(url, info);
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
		else {
			throw new SQLException("Invalid JDBC URL. The driver URL must follow this format" +
					" 'jdbc:gd://<gooddata-server>/gdc/projects/<workspace-id>'.");
		}
	}

	@Override
	public boolean acceptsURL(String url) {
    	logger.info("jdbc4gd: acceptsURL url:"+url);
		return url.startsWith("jdbc:gd:");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		logger.info("jdbc4gd: getPropertyInfo");
		throw new SQLFeatureNotSupportedException("Sorry, getPropertyInfo call isn't supported yet.");
	}

	@Override
	public int getMajorVersion() {
		logger.info("jdbc4gd: getMajorVersion");
		return MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion() {
		logger.info("jdbc4gd: getMinorVersion");
		return MINOR_VERSION;
	}

	@Override
	public boolean jdbcCompliant() {
		logger.info("jdbc4gd: jdbcCompliant");
		return false;
	}

	@Override
	public Logger getParentLogger() {
		logger.info("jdbc4gd: getParentLogger");
		return logger;
	}

}
