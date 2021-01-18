package com.gooddata.jdbc.metadata;

import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.parser.SQLParser;
import com.gooddata.jdbc.util.TextUtil;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

/**
 * AFM ResultSet JDBC metadata
 */
public class AfmResultSetMetaData extends AbstractResultSetMetaData implements ResultSetMetaData {

	public final static String UNIVERSAL_TABLE_NAME = "LDM";
	public final static String UNIVERSAL_CATALOG_NAME = "";

	private final static Logger LOGGER = Logger.getLogger(AfmResultSetMetaData.class.getName());

	private final List<CatalogEntry> columns;

	/**
	 * Constructor
	 * @param columns AFM catalog
	 */
	public AfmResultSetMetaData(List<CatalogEntry> columns) {
		this.columns = columns;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getColumnCount() {
		return columns.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getColumnLabel(int column) throws SQLException {
		return this.getColumnName(column);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getColumnName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		return columns.get(column-1).getTitle();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getSchemaName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		return TextUtil.extractIdFromUri(this.columns.get(column - 1).getUri());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPrecision(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		CatalogEntry c = columns.get(column - 1);
		return c.getPrecision();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getScale(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		CatalogEntry c = columns.get(column-1);
		return c.getSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTableName(int column)  {
		return UNIVERSAL_TABLE_NAME;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getCatalogName(int column) {
		return UNIVERSAL_CATALOG_NAME;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getColumnType(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		CatalogEntry c = columns.get(column-1);
		return SQLParser.convertSQLDataTypeNameToJavaSQLType(c.getDataType());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getColumnTypeName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		CatalogEntry c = columns.get(column-1);
		return c.getDataType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getColumnClassName(int column) throws SQLException {
		if(column <= 0 || column > this.columns.size())
			throw new SQLException(String.format("Column index %d column out of range.", column));
		CatalogEntry c = columns.get(column-1);
		return SQLParser.convertSQLDataTypeNameToJavaClassName(c.getDataType());
	}

}
