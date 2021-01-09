package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.DataTypeParser;
import com.gooddata.sdk.model.executeafm.result.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * ResultSet table
 */
public class ResultSetTable implements ResultSet {

	private final static Logger logger = Logger.getGlobal();//Logger.getLogger(ResultSetTable.class.getName());
	private final ExecutionResult afmExecutionResult;
	private final List<com.gooddata.jdbc.driver.Statement.CatalogEntry> columns;
	private final Statement statement;

	private int currentIndex = -1;

	/**
	 * Constructor
	 * @param statement SQL statement
	 * @param result GD AFM execution result
	 * @param columns AFM columns
	 */
	public ResultSetTable(Statement statement, ExecutionResult result,
						  List<com.gooddata.jdbc.driver.Statement.CatalogEntry> columns) {
		this.afmExecutionResult = result;
		this.columns = columns;
		this.statement = statement;
	}
	@Override
	public java.sql.ResultSetMetaData getMetaData() throws SQLException {
		return null;
	}

	/**
	 * Get textual value
	 * @param columnName name of the column
	 * @return textual value
	 * @throws SQLException in case of issues
	 */
	public String getTextValue(String columnName) throws SQLException {
		return getTextValue(this.findColumn(columnName));
	}

	/**
	 * Get textual value
	 * @param columnIndex 1 based index
	 * @return textual value
	 * @throws SQLException in case of issues
	 */
	public String getTextValue(int columnIndex) throws SQLException {
		int realIndex = columnIndex - 1;
		if( realIndex >= this.columns.size() )
			throw new SQLException("Column index too high.");
		List<List<List<ResultHeaderItem>>> headers = this.afmExecutionResult.getHeaderItems();
		if(headers.size() == 2) {
			List<List<ResultHeaderItem>> headerColumns = headers.get(0);;
			List<Data> numbers = this.afmExecutionResult.getData();
			if(realIndex < headerColumns.size()) {
				return headerColumns.get(realIndex).get(this.currentIndex).getName();
			}
			else {
				return numbers.get(this.currentIndex).asList().get(realIndex - headerColumns.size()).textValue();
			}
		}
		else {
			List<Data> numbers = this.afmExecutionResult.getData();
			return numbers.get(this.currentIndex).textValue();
		}
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Move cursor to the next position
	 * @return true if the move to the next row is successful
	 * @throws SQLException in case of issues
	 */
	@Override
	public boolean next() throws SQLException {
		logger.info("jdbc4gd: resultsettable next position:");
		return relative(1);
	}

	@Override
	public void close() {
	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not implemented yet.");
	}

	/**
	 * Get textual value
	 * @param columnIndex 1 based index
	 * @return textual value
	 * @throws SQLException in case of issues
	 */
	@Override
	public String getString(int columnIndex) throws SQLException {
		return getTextValue(columnIndex);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return null;
	}

	/**
	 * Finds 1-based column index by column name
	 * @param columnLabel column name
	 * @return the 1-based column index
	 * @throws SQLException if the column doesn't exist
	 */
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		int index = this.columns.stream().map(c->c.getTitle()).collect(Collectors.toList()).indexOf(columnLabel);
		if(index >= 0)
			return index + 1;
		else
			throw new SQLException(String.format("Column '%s' doesn't exist.", columnLabel));
	}

	private static final List<String> FALSE_VALUES = Arrays.asList("0", "false", "f");
	private static final List<String> TRUE_VALUES = Arrays.asList("1", "true", "t");

	private boolean containsIgnoreCase(List<String> l, String s) {
		return l.stream().anyMatch(s::equalsIgnoreCase);
	}

	/**
	 * Return boolean value
	 * @param columnIndex 1-based column index
	 * @return boolean value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return DataTypeParser.parseBoolean(getTextValue(columnIndex));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return 0;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return 0;
	}

	/**
	 * Return integer value
	 * @param columnIndex 1-based column index
	 * @return integer value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public int getInt(int columnIndex) throws SQLException {
		return DataTypeParser.parseInt(getTextValue(columnIndex));
	}

	/**
	 * Return long value
	 * @param columnIndex 1-based column index
	 * @return long value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public long getLong(int columnIndex) throws SQLException {
		return DataTypeParser.parseLong(getTextValue(columnIndex));
	}

	/**
	 * Return float value
	 * @param columnIndex 1-based column index
	 * @return float value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return DataTypeParser.parseFloat(getTextValue(columnIndex));
	}

	/**
	 * Return double value
	 * @param columnIndex 1-based column index
	 * @return double value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return DataTypeParser.parseDouble(getTextValue(columnIndex));
	}

	/**
	 * Return BigDecimal value
	 * @param columnIndex 1-based column index
	 * @param scale precision
	 * @return BigDecimal value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return DataTypeParser.parseBigDecimal(getTextValue(columnIndex), scale);
	}

	/**
	 * Return BigDecimal value
	 * @param columnIndex 1-based column index
	 * @return BigDecimal value
	 * @throws SQLException in case when the value cannot be converted
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return DataTypeParser.parseBigDecimal(getTextValue(columnIndex));
	}


	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return null;
	}

	/**
	 * Get String value
	 * @param columnLabel name of the column
	 * @return String value
	 * @throws SQLException in case of issues
	 */
	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(this.findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {

		return getBigDecimal(this.findColumn(columnLabel));
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getString(this.findColumn(columnLabel));
	}

	/**
	 * Get Boolean value
	 * @param columnLabel name of the column
	 * @return bool value
	 * @throws SQLException in case of issues
	 */
	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(this.findColumn(columnLabel));
	}

	/**
	 * Get Byte value
	 * @param columnLabel name of the column
	 * @return byte value
	 * @throws SQLException in case of issues
	 */
	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(this.findColumn(columnLabel));
	}

	/**
	 * Get short value
	 * @param columnLabel name of the column
	 * @return short value
	 * @throws SQLException in case of issues
	 */
	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(this.findColumn(columnLabel));
	}

	/**
	 * Get int value
	 * @param columnLabel name of the column
	 * @return int value
	 * @throws SQLException in case of issues
	 */
	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(this.findColumn(columnLabel));
	}

	/**
	 * Get long value
	 * @param columnLabel name of the column
	 * @return long value
	 * @throws SQLException in case of issues
	 */
	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(this.findColumn(columnLabel));
	}

	/**
	 * Get float value
	 * @param columnLabel name of the column
	 * @return float value
	 * @throws SQLException in case of issues
	 */
	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(this.findColumn(columnLabel));
	}

	/**
	 * Get double value
	 * @param columnLabel name of the column
	 * @return double value
	 * @throws SQLException in case of issues
	 */
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(this.findColumn(columnLabel));
	}

	/**
	 * Get BigDecimal value
	 * @param columnLabel name of the column
	 * @param scale scale
	 * @return BigDecimal value
	 * @throws SQLException in case of issues
	 */
	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(this.findColumn(columnLabel), scale);
	}

	/**
	 * Get byte array value
	 * @param columnLabel name of the column
	 * @return byte array value
	 * @throws SQLException in case of issues
	 */
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(this.findColumn(columnLabel));
	}

	/**
	 * Get Date value
	 * @param columnLabel name of the column
	 * @return Date value
	 * @throws SQLException in case of issues
	 */
	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(this.findColumn(columnLabel));
	}

	/**
	 * Get Time value
	 * @param columnLabel name of the column
	 * @return Time value
	 * @throws SQLException in case of issues
	 */
	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(this.findColumn(columnLabel));
	}

	/**
	 * Get Timestamp value
	 * @param columnLabel name of the column
	 * @return Timestamp value
	 * @throws SQLException in case of issues
	 */
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(this.findColumn(columnLabel));
	}

	@Override
	public String getCursorName() throws SQLException {
		return "GD";
	}

	@Override
	public Statement getStatement() throws SQLException {
		return this.statement;
	}

	@Override
	public int getHoldability() throws SQLException {
		return CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public boolean isBeforeFirst() {
		return this.currentIndex == -1;
	}

	@Override
	public boolean isAfterLast() {
		return this.currentIndex >= this.afmExecutionResult.getData().size();
	}

	@Override
	public boolean isFirst()  {
		return this.currentIndex == 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		return this.currentIndex == this.afmExecutionResult.getData().size() - 1;
	}

	@Override
	public void beforeFirst() {
		this.currentIndex = -1;
	}

	@Override
	public void afterLast() {
		this.currentIndex = this.afmExecutionResult.getData().size();
	}

	@Override
	public boolean first() {
		this.currentIndex = 0;
		return true;
	}

	@Override
	public boolean last() {
		this.currentIndex = this.afmExecutionResult.getData().size() - 1;
		return true;
	}

	@Override
	public int getRow() {
		return this.currentIndex;
	}

	@Override
	public boolean absolute(int row) {
		if(row >= 0 && row < this.afmExecutionResult.getData().size()) {
			this.currentIndex = row;
			return true;
		}
		return false;
	}

	@Override
	public boolean relative(int rowsIncrement) {
		return absolute(this.currentIndex + rowsIncrement);
	}

	@Override
	public boolean previous() {
		return relative(-1);
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not implemented yet");
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not implemented yet");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public int getType() throws SQLException {
		return TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return CONCUR_READ_ONLY;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	/**
	 * Get ascii stream value
	 * @param columnLabel name of the column
	 * @return ascii stream value
	 * @throws SQLException in case of issues
	 */
	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(this.findColumn(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(this.findColumn(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(this.findColumn(columnLabel));
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet");
	}

}
