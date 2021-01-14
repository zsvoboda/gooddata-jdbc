package com.gooddata.jdbc.util;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Many boilerplate JDBC methods implementation
 */
public abstract class AbstractResultSet implements java.sql.ResultSet {


    private final static Logger LOGGER = Logger.getLogger(AbstractResultSet.class.getName());

    public static final int HOLDABILITY = CLOSE_CURSORS_AT_COMMIT;
    public static final int FETCH_DIRECTION = FETCH_FORWARD;
    public static final int CONCURRENCY = CONCUR_READ_ONLY;
    public static final int TYPE = TYPE_SCROLL_INSENSITIVE;

    /**
     * Constructor
     */
    public AbstractResultSet() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public java.sql.ResultSetMetaData getMetaData() throws SQLException;

    /**
     * Get textual value
     *
     * @param columnName name of the column
     * @return textual value
     * @throws SQLException in case of issues
     */
    public String getTextValue(String columnName) throws SQLException {
        return getTextValue(this.findColumn(columnName));
    }

    /**
     * Get textual value
     *
     * @param columnIndex 1 based index
     * @return textual value
     * @throws SQLException in case of issues
     */
    abstract public String getTextValue(int columnIndex) throws SQLException;

    /**
     * Finds column index by column name
     *
     * @param columnLabel column name
     * @return column index 1-based
     * @throws SQLException if something goes wrong
     */
    abstract public int findColumn(String columnLabel) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate(int columnIndex, Calendar cal) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate(String columnLabel, Calendar cal) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Time getTime(int columnIndex, Calendar cal) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Time getTime(String columnLabel, Calendar cal) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean next() {
        return relative(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(int columnIndex) throws SQLException {
        return getTextValue(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return (T) getObject(columnIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return DataTypeParser.parseBoolean(getTextValue(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int columnIndex) {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(int columnIndex) {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int columnIndex) throws SQLException {
        return DataTypeParser.parseInt(getTextValue(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int columnIndex) throws SQLException {
        return DataTypeParser.parseLong(getTextValue(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return DataTypeParser.parseFloat(getTextValue(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return DataTypeParser.parseDouble(getTextValue(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return DataTypeParser.parseBigDecimal(getTextValue(columnIndex), scale);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return DataTypeParser.parseBigDecimal(getTextValue(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        ResultSetMetaData m = this.getMetaData();
        return DataTypeParser.parseObject(getTextValue(columnIndex),
                m.getColumnType(columnIndex), m.getPrecision(columnIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return (T) getObject(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {

        return getBigDecimal(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(this.findColumn(columnLabel), scale);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCursorName() {
        return "GD";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public Statement getStatement() throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHoldability() {
        return HOLDABILITY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean isBeforeFirst();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean isAfterLast();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean isFirst();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean isLast() throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public void beforeFirst();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public void afterLast();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean first();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean last();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public int getRow();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean absolute(int row);

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public boolean relative(int rowsIncrement);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean previous() {
        return relative(-1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getWarnings is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.clearWarnings is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.setFetchDirection is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getFetchDirection is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.setFetchSize is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getFetchSize is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE_SCROLL_INSENSITIVE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getConcurrency is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getUnicodeStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getBinaryStream is not implemented yet");
    }

    /**
     * Get ascii stream value
     * @param columnLabel name of the column
     * @return ascii stream value
     * @throws SQLException in case of issues
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(this.findColumn(columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.rowUpdated is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.rowInserted is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.rowDeleted is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNull is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBoolean is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateByte is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateShort is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateInt is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateLong is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateFloat is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateDouble is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBigDecimal is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateString is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBytes is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateDate is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateTime is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateTimestamp is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBinaryStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateObject is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateObject is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNull is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBoolean is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateByte is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateShort is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateInt is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateLong is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateFloat is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateDouble is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBigDecimal is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateString is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBytes is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateDate is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateTime is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateTimestamp is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBinaryStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateObject is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateObject is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.insertRow is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateRow is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.deleteRow is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.refreshRow is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.cancelRowUpdates is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.moveToInsertRow is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.moveToCurrentRow is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getRef is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getArray is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return this.getObject(columnLabel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getRef is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getArray is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getURL is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getURL is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateRef is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateRef is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateArray is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateArray is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getRowId is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getRowId is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateRowId is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateRowId is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNString is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNString is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getSQLXML is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getSQLXML is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateSQLXML is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateSQLXML is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getNString is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getNString is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getNCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.getNCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBinaryStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBinaryStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBinaryStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateAsciiStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBinaryStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateCharacterStream is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateBlob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.updateNClob is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.unwrap is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("AbstractResultSet.isWrapperFor is not implemented yet");
    }

}
