package com.gooddata.jdbc.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Boilerplate ResultSetMetadata methods
 */
public abstract class AbstractResultSetMetaData implements ResultSetMetaData {

    private final static Logger LOGGER = Logger.getLogger(AbstractResultSetMetaData.class.getName());

	/**
	 * Constructor
	 */
	public AbstractResultSetMetaData() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public int getColumnCount();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSearchable(int column) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int isNullable(int column) {
        return columnNullableUnknown;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSigned(int column) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSetMetaData.getColumnDisplaySize is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getColumnLabel(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getColumnName(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getSchemaName(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public int getPrecision(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public int getScale(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getTableName(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getCatalogName(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public int getColumnType(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getColumnTypeName(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWritable(int column) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public String getColumnClassName(int column) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("AbstractResultSetMetaData.unwrap is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("AbstractResultSetMetaData.isWrapperFor is not implemented yet");
    }

}
