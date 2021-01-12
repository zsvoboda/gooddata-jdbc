package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.AbstractResultSetMetaData;
import com.gooddata.jdbc.util.DataTypeParser;
import com.gooddata.jdbc.util.TextUtil;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
		return c.getPrecision();
	}

	@Override
	public int getScale(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		return c.getSize();
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
		return DataTypeParser.convertSQLDataTypeNameToJavaSQLType(c.getDataType());
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		return c.getDataType();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		DatabaseMetaData.CatalogEntry c = columns.get(column-1);
		return DataTypeParser.convertSQLDataTypeNameToJavaClassName(c.getDataType());
	}

}
