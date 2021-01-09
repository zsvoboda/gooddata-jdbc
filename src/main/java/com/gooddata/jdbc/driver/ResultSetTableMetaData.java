package com.gooddata.jdbc.driver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetTableMetaData implements ResultSetMetaData {

	private final String [] columns;
	private final boolean [] isNumeric;
	
	public ResultSetTableMetaData(final String [] header, final boolean[] isNumeric) {
		this.columns = header;
		this.isNumeric=isNumeric;
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getColumnCount() throws SQLException {
		
		return columns.length;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		
		return (columns[column-1].length()>20)? columns[column-1].length() : 20;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return columns[column-1];
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return columns[column-1];
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		if (isNumeric[column-1])
		{
			return java.sql.Types.NUMERIC;
		}
		else
		{
		    return java.sql.Types.VARCHAR;
		}
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		// TODO Auto-generated method stub
		if (isNumeric[column-1])
		{
			return "NUMERIC";
		}
		else
		{
		    return "VARCHAR";
		}
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		if (isNumeric[column-1])
		{
			return "java.lang.Number";
		}
		else
		{
		    return "java.lang.String";
		}
		
	}

}
