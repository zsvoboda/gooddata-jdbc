package com.gooddata.jdbc.driver;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {

	private final static Logger LOGGER = Logger.getLogger(Driver.class.getName());;

	public final static int MAJOR_VERSION = 0;
	public final static int MINOR_VERSION = 5;
	public final static String VERSION = String.format("%x.%x", MAJOR_VERSION, MINOR_VERSION);

	static void setupLogging() throws IOException {
		Logger log = Logger.getGlobal();
		FileInputStream fis =  new FileInputStream(String.format("%s/.logging",
			System.getProperty("user.home")));
		LogManager.getLogManager().readConfiguration(fis);
		log.setUseParentHandlers(false);
		fis.close();
	}

	 static {

		 try {
		 		setupLogging();
	        	LOGGER.info("GooodData JDBC Driver started");
	            DriverManager.registerDriver(new Driver());
	        } catch (IOException | SQLException e) {
	            throw new RuntimeException(e);
	        }
	    }

	/**
	 * Default constructor
	 */
    public Driver() {}

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
		return url.startsWith("jdbc:gd:");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		throw new SQLFeatureNotSupportedException("Sorry, getPropertyInfo call isn't supported yet.");
	}

	@Override
	public int getMajorVersion() {
		return MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion() {
		return MINOR_VERSION;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() {
		return LOGGER;
	}

}
