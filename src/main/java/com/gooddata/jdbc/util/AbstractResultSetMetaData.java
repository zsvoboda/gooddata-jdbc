package com.gooddata.jdbc.util;

import com.gooddata.jdbc.driver.DatabaseMetaData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.logging.Logger;

public abstract class AbstractResultSetMetaData implements ResultSetMetaData {

	private final static Logger LOGGER = Logger.getLogger(AbstractResultSetMetaData.class.getName());

	public AbstractResultSetMetaData() {
	}

	@Override
	abstract public int getColumnCount();

	@Override
	public boolean isAutoIncrement(int column) {
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) {
		return true;
	}

	@Override
	public boolean isSearchable(int column) {
		return false;
	}

	@Override
	public boolean isCurrency(int column) {
		return false;
	}

	@Override
	public int isNullable(int column)  {
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) {
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("AbstractResultSetMetaData.getColumnDisplaySize is not implemented yet");
	}

	@Override
	abstract public String getColumnLabel(int column) throws SQLException;

	@Override
	abstract public String getColumnName(int column) throws SQLException;

	@Override
	abstract public String getSchemaName(int column) throws SQLException;

	@Override
	abstract public int getPrecision(int column) throws SQLException;

	@Override
	abstract public int getScale(int column) throws SQLException;

	@Override
	abstract public String getTableName(int column) throws SQLException;

	@Override
	abstract public String getCatalogName(int column) throws SQLException;

	@Override
	abstract public int getColumnType(int column) throws SQLException;

	@Override
	abstract public String getColumnTypeName(int column) throws SQLException;

	@Override
	public boolean isReadOnly(int column) {
		return true;
	}

	@Override
	public boolean isWritable(int column) {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) {
		return false;
	}

	@Override
	abstract public String getColumnClassName(int column) throws SQLException;

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException{
		throw new SQLFeatureNotSupportedException("AbstractResultSetMetaData.unwrap is not implemented yet");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface)  throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("AbstractResultSetMetaData.isWrapperFor is not implemented yet");
	}

}
