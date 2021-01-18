package com.gooddata.jdbc.resultset;

import com.gooddata.jdbc.metadata.MetadataResultSetMetaData;
import com.gooddata.jdbc.util.AbstractResultSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AFM metadata ResultSet wrapper
 */
public class MetadataResultSet extends AbstractResultSet {

    private int currentIndex = -1;
    private final List<MetaDataColumn> data;
    private final int rowCount;
    private final ResultSetMetaData metadata;

    /**
     * Metadata column
     */
    public static class MetaDataColumn {

        private String name;
        private String dataType;

        /**
         * Constructor
         *
         * @param name column name
         * @param data column data
         */
        public MetaDataColumn(String name, List<String> data) {
            this(name, "VARCHAR", data);
        }

        /**
         * Constructor
         *
         * @param name     column name
         * @param dataType column datatype
         * @param data     column data
         */
        public MetaDataColumn(String name, String dataType, List<String> data) {
            this.name = name;
            this.dataType = dataType;
            this.values = data;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }


        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            this.values = values;
        }

        private List<String> values;
    }

    /**
     * Constructor
     *
     * @param data metadata dataset columns
     */
    public MetadataResultSet(List<MetaDataColumn> data) {
        this.data = data;
        int minLength = 0;
        for (MetaDataColumn column : data) {
            int colLength = column.getValues().size();
            if (minLength == 0)
                minLength = colLength;
            else
                minLength = Math.min(colLength, minLength);
        }
        this.rowCount = minLength;
        this.metadata = new MetadataResultSetMetaData(this.data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetMetaData getMetaData() {
        return this.metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTextValue(int columnIndex) throws SQLException {
        if (this.currentIndex < 0 || this.currentIndex >= this.rowCount)
            throw new SQLException("Cursor is out of range.");
        int realIndex = columnIndex - 1;
        if (realIndex >= this.data.size())
            throw new SQLException("Column index too high.");
        return data.get(columnIndex - 1).getValues().get(this.currentIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int index = this.data.stream().map(MetaDataColumn::getName).collect(Collectors.toList()).indexOf(columnLabel);
        if (index >= 0)
            return index + 1;
        else
            throw new SQLException(String.format("Column '%s' doesn't exist.", columnLabel));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement getStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException("MetadataResultSet.getStatement is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isBeforeFirst() {
        return this.currentIndex == -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAfterLast() {
        return this.currentIndex >= this.rowCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isFirst() {
        return this.currentIndex == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLast() {
        return this.currentIndex == this.rowCount - 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeFirst() {
        this.currentIndex = -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterLast() {
        this.currentIndex = this.rowCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean first() {
        this.currentIndex = 0;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean last() {
        this.currentIndex = this.rowCount - 1;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRow() {
        return Math.max(this.currentIndex, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean absolute(int row) {
        if (row >= 0 && row < this.rowCount) {
            this.currentIndex = row;
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean relative(int rowsIncrement) {
        return absolute(this.currentIndex + rowsIncrement);
    }

}
