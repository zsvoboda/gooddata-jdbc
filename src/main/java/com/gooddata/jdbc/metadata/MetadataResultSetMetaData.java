package com.gooddata.jdbc.metadata;

import com.gooddata.jdbc.resultset.MetadataResultSet;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class MetadataResultSetMetaData extends AbstractResultSetMetaData {

    public final static String UNIVERSAL_METADATA_CATALOG_NAME = "GDCATALOG";
    public final static String UNIVERSAL_METADATA_SCHEMA_NAME = "GDMETADATA";
    public final static String UNIVERSAL_METADATA_TABLE_NAME = "GDSYSTEM";

    private final List<MetadataResultSet.MetaDataColumn> data;

    public MetadataResultSetMetaData(List<MetadataResultSet.MetaDataColumn> data) {
        this.data = data;
    }

    @Override
    public int getColumnCount() {
        return this.data.size();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return this.getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        if(column <= 0 || column > data.size())
            throw new SQLException(String.format("Column index %d column out of range.", column));
        return data.get(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) {
        return UNIVERSAL_METADATA_SCHEMA_NAME;
    }

    @Override
    public int getPrecision(int column) {
        return 255;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return UNIVERSAL_METADATA_TABLE_NAME;
    }

    @Override
    public String getCatalogName(int column) {
        return UNIVERSAL_METADATA_CATALOG_NAME;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        if(column <= 0 || column > data.size())
            throw new SQLException(String.format("Column index %d column out of range.", column));
        if(this.data.get(column-1).getDataType().equals("INTEGER"))
            return Types.INTEGER;
        return java.sql.Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if(column <= 0 || column > data.size())
            throw new SQLException(String.format("Column index %d column out of range.", column));
        if(this.data.get(column-1).getDataType().equals("INTEGER"))
            return "INTEGER";
        return "VARCHAR";
    }

    @Override
    public String getColumnClassName(int column) throws SQLException{
        if(column <= 0 || column > data.size())
            throw new SQLException(String.format("Column index %d column out of range.", column));
        if(this.data.get(column-1).getDataType().equals("INTEGER"))
            return "java.lang.Integer";
        return "java.lang.String";
    }
}
