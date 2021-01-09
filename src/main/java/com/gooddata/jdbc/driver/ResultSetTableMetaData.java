package com.gooddata.jdbc.driver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

public class ResultSetTableMetaData implements ResultSetMetaData {

	private final List<Statement.CatalogEntry> columns;
	
	public ResultSetTableMetaData(List<Statement.CatalogEntry> columns) {
		this.columns = columns;
	}
	

	@Override
	public int getColumnCount() {
		return columns.size();
	}

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
		throw new SQLFeatureNotSupportedException("Not yet implemented.");
	}

	@Override
	public String getColumnLabel(int column) {
		return columns.get(column-1).getTitle();
	}

	@Override
	public String getColumnName(int column) {
		return columns.get(column-1).getTitle();
	}

	@Override
	public String getSchemaName(int column)  {
		return "GD";
	}

	@Override
	public int getPrecision(int column) {
		Statement.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return 5;
		else
			return 0;
	}

	@Override
	public int getScale(int column) {
		Statement.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return 15;
		else
			return 255;
	}

	@Override
	public String getTableName(int column)  {
		return "DATA";
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not yet implemented.");
	}

	@Override
	public int getColumnType(int column) {
		Statement.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return java.sql.Types.NUMERIC;
		else
			return java.sql.Types.VARCHAR;
	}

	@Override
	public String getColumnTypeName(int column) {
		Statement.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return "NUMERIC";
		else
			return "VARCHAR";
	}

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
	public String getColumnClassName(int column) {
		Statement.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return "java.lang.Number";
		else
			return "java.lang.String";
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException{
		throw new SQLFeatureNotSupportedException("Not yet implemented.");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface)  throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("Not yet implemented.");
	}


}
