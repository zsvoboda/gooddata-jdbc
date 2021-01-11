package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.AbstractResultSetMetaData;
import com.gooddata.jdbc.util.TextUtil;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.logging.Logger;

public class ResultSetTableMetaData extends AbstractResultSetMetaData implements ResultSetMetaData {

	public final static String UNIVERSAL_TABLE_NAME = "LDM";
	public final static String UNIVERSAL_CATALOG_NAME = "";

	private final static Logger LOGGER = Logger.getLogger(ResultSetTableMetaData.class.getName());

	private final List<DatabaseMetaData.CatalogEntry> columns;
	
	public ResultSetTableMetaData(List<DatabaseMetaData.CatalogEntry> columns) {
		this.columns = columns;
	}

	@Override
	public int getColumnCount() {
		return columns.size();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return this.getColumnName(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		return columns.get(column-1).getTitle();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		return TextUtil.extractWorkspaceIdFromUri(this.columns.get(column - 1).getUri());
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column - 1);
		if(c.getType().equalsIgnoreCase("metric"))
			return 5;
		else
			return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return 15;
		else
			return 255;
	}

	@Override
	public String getTableName(int column)  {
		return UNIVERSAL_TABLE_NAME;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return UNIVERSAL_CATALOG_NAME;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return java.sql.Types.NUMERIC;
		else
			return java.sql.Types.VARCHAR;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return "NUMERIC";
		else
			return "VARCHAR";
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		if(c.getType().equalsIgnoreCase("metric"))
			return "java.lang.Number";
		else
			return "java.lang.String";
	}

}
