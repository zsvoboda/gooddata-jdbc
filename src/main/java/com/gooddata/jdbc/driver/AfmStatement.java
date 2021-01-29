package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.catalog.AfmFilter;
import com.gooddata.jdbc.catalog.Catalog;
import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.metadata.AfmDatabaseMetaData;
import com.gooddata.jdbc.parser.MaqlParser;
import com.gooddata.jdbc.parser.SQLParser;
import com.gooddata.jdbc.resultset.AbstractResultSet;
import com.gooddata.jdbc.resultset.AfmResultSet;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.executeafm.afm.Afm;
import com.gooddata.sdk.model.executeafm.afm.AttributeItem;
import com.gooddata.sdk.model.executeafm.afm.MeasureItem;
import com.gooddata.sdk.model.executeafm.afm.SimpleMeasureDefinition;
import com.gooddata.sdk.model.executeafm.resultspec.SortItem;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.executeafm.ExecuteAfmService;
import com.gooddata.sdk.service.md.MetadataService;
import net.sf.jsqlparser.JSQLParserException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Logger;

/**
 * JDBC statement
 */
public class AfmStatement implements java.sql.Statement, PreparedStatement {

    private final static Logger LOGGER = Logger.getLogger(AfmStatement.class.getName());

    private final Project workspace;
    private final AfmConnection afmConnection;
    private final AfmDatabaseMetaData metadata;
    private final ExecuteAfmService gdAfm;
    private final MetadataService gdMeta;

    private final Map<Integer, Object> preparedStatementParams = new HashMap<>();

    private boolean isClosed = false;
    private AfmResultSet resultSet;
    private int maxRows = 0;

    private String sql;
    private int queryTimeout;

    private int fetchSize = 0;


    /**
     * Constructor
     *
     * @param con      java.sql.Connection
     * @param gd       GoodData connection class
     * @param metadata database metadata
     */
    public AfmStatement(AfmConnection con, GoodData gd, AfmDatabaseMetaData metadata, String sql) {
        LOGGER.info("AfmStatement");
        this.afmConnection = con;
        this.metadata = metadata;
        this.sql = sql;
        this.workspace = metadata.getWorkspace();
        this.gdAfm = gd.getExecuteAfmService();
        this.gdMeta = gd.getMetadataService();
    }

    /**
     * Populates AFM execution parameter
     *
     * @param columns AFM columns
     * @param filters AFM filters
     * @return AFM object
     * @throws Catalog.DuplicateCatalogEntryException when there are multiple LDM object with a named mentioned in the parsed SQL
     * @throws Catalog.CatalogEntryNotFoundException  when a SQL object (column) can't be resolved
     */
    private Afm getAfm(List<CatalogEntry> columns, List<AfmFilter> filters) throws Catalog.DuplicateCatalogEntryException,
            Catalog.CatalogEntryNotFoundException {
        LOGGER.info(String.format("getAfm columns='%s', filters='%s'", columns, filters));
        Afm afm = new Afm();
        for (CatalogEntry o : columns) {
            if (o.getType().equalsIgnoreCase("attributeDisplayForm")) {
                afm.addAttribute(new AttributeItem(o.getGdObject(), o.getIdentifier()));
            } else if (o.getType().equalsIgnoreCase("metric")) {
                afm.addMeasure(new MeasureItem(new SimpleMeasureDefinition(o.getGdObject()),
                        o.getIdentifier()));
            }
        }
        for (AfmFilter f : filters) {
            afm.addFilter(f.getFilterObj());
        }
        return afm;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        LOGGER.info(String.format("executeQuery sql='%s'", sql));
        try {
            this.sql = sql;
            SQLParser parser = new SQLParser();
            SQLParser.ParsedSQL parsedSql = parser.parseQuery(sql);
            return execute(parsedSql);
        } catch (JSQLParserException | Catalog.CatalogEntryNotFoundException
                | Catalog.DuplicateCatalogEntryException | TextUtil.InvalidFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            SQLParser parser = new SQLParser();
            SQLParser.ParsedSQL parsedSql = parser.parseQuery(this.sql);
            // TBD replace prepared sql parameters here
            return execute(parsedSql);
        } catch (JSQLParserException | Catalog.CatalogEntryNotFoundException
                | Catalog.DuplicateCatalogEntryException | TextUtil.InvalidFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Execute either regular or prepared statement
     * @param parsedSql parsed SQL
     * @return ResultSet
     * @throws Catalog.DuplicateCatalogEntryException in case of duplicate catalog item
     * @throws Catalog.CatalogEntryNotFoundException in case of non-existent catalog item
     * @throws TextUtil.InvalidFormatException in case of invalid datetype format
     * @throws SQLException other problems
     */
    private ResultSet execute(SQLParser.ParsedSQL parsedSql) throws Catalog.DuplicateCatalogEntryException,
            Catalog.CatalogEntryNotFoundException, TextUtil.InvalidFormatException, SQLException {
        List<CatalogEntry> columns = this.metadata.getCatalog().resolveAfmColumns(parsedSql);
        List<AfmFilter> filters = this.metadata.getCatalog().resolveAfmFilters(parsedSql);
        List<SortItem> orderBys = this.metadata.getCatalog().resolveOrderBys(parsedSql, columns);
        Afm afm = getAfm(columns, filters);
        return new AfmResultSet(this, this.workspace, this.gdAfm, afm, columns, orderBys,
                parsedSql.getLimit(), parsedSql.getOffset());
    }

    @Override
    public boolean execute() throws SQLException {
        this.resultSet = (AfmResultSet) this.executeQuery();
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        LOGGER.info(String.format("execute sql='%s'", sql));
        try {
            if (sql.trim().toLowerCase().startsWith("create")) {
                MaqlParser parser = new MaqlParser();
                MaqlParser.ParsedCreateMetricStatement parsedCreate
                        = parser.parseCreateOrAlterMetric(sql);
                this.executeCreateMetric(parsedCreate);
                return false;
            } else if (sql.trim().toLowerCase().startsWith("alter")) {
                MaqlParser parser = new MaqlParser();
                MaqlParser.ParsedCreateMetricStatement parsedCreate
                        = parser.parseCreateOrAlterMetric(sql);
                this.executeAlterMetric(parsedCreate);
                return false;
            } else if (sql.trim().toLowerCase().startsWith("drop")) {
                MaqlParser parser = new MaqlParser();
                String parsedDropMetric = parser.parseDropMetric(sql);
                this.executeDropMetric(parsedDropMetric);
                return false;
            } else {
                this.resultSet = (AfmResultSet) this.executeQuery(sql);
                return true;
            }
        } catch (Catalog.CatalogEntryNotFoundException |
                Catalog.DuplicateCatalogEntryException
                | JSQLParserException | TextUtil.InvalidFormatException e) {
            throw new SQLException(e);
        }

    }

    /**
     * Execute CREATE METRIC statement
     *
     * @param parsedMaqlCreate CREATE METRIC statement
     * @throws Catalog.CatalogEntryNotFoundException  issues with resolving referenced objects
     * @throws Catalog.DuplicateCatalogEntryException issues with resolving referenced objects
     * @throws TextUtil.InvalidFormatException        invalid format of URI
     */
    public void executeCreateMetric(MaqlParser.ParsedCreateMetricStatement parsedMaqlCreate) throws
            Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException,
            TextUtil.InvalidFormatException {
        LOGGER.info(String.format("executeCreateMetric parsedMaqlCreate='%s'", parsedMaqlCreate));
        String maqlDefinition = this.metadata.getGoodDataRestConnection()
                .replaceMaqlTitlesWithUris(parsedMaqlCreate, this.metadata.getCatalog());
        // TODO format
        Metric m = new Metric(parsedMaqlCreate.getName(),
                maqlDefinition,
                "###,###.00");
        Metric newMetric = this.gdMeta.createObj(this.workspace, m);
        this.metadata.getCatalog().addMetric(newMetric);
    }

    /**
     * Execute ALTER METRIC statement
     *
     * @param parsedMaqlCreate ALTER METRIC statement
     * @throws Catalog.CatalogEntryNotFoundException  issues with resolving referenced objects
     * @throws Catalog.DuplicateCatalogEntryException issues with resolving referenced objects
     */
    public void executeAlterMetric(MaqlParser.ParsedCreateMetricStatement parsedMaqlCreate) throws
            Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException,
            SQLException, TextUtil.InvalidFormatException {
        LOGGER.info(String.format("executeAlterMetric parsedMaqlCreate='%s'", parsedMaqlCreate));
        String maqlDefinition = this.metadata.getGoodDataRestConnection()
                .replaceMaqlTitlesWithUris(parsedMaqlCreate, this.metadata.getCatalog());

        CatalogEntry entry = this.metadata.getCatalog().findAfmColumn(parsedMaqlCreate.getName());
        Metric m = this.gdMeta.getObjByUri(entry.getUri(), Metric.class);
        this.metadata.getGoodDataRestConnection().updateMetric(m, maqlDefinition);
    }


    /**
     * Execute DROP METRIC statement
     *
     * @param metricName dropped metric name
     * @throws Catalog.CatalogEntryNotFoundException  issues with resolving referenced objects
     * @throws Catalog.DuplicateCatalogEntryException issues with resolving referenced objects
     */
    public void executeDropMetric(String metricName) throws
            Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException, TextUtil.InvalidFormatException {
        LOGGER.info(String.format("executeDropMetric metricName='%s'", metricName));
        CatalogEntry ldmObj = this.metadata.getCatalog().findAfmColumn(metricName);
        this.gdMeta.removeObjByUri(ldmObj.getUri());
        this.metadata.getCatalog().remove(ldmObj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.ResultSet getResultSet() {
        LOGGER.info("getResultSet");
        return this.resultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        LOGGER.info(String.format("execute sql='%s', autoGeneratedKeys='%s'", sql, autoGeneratedKeys));
        return execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        LOGGER.info(String.format("execute sql='%s', columnIndexes='%s'", sql, Arrays.toString(columnIndexes)));
        return execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        LOGGER.info(String.format("execute sql='%s', columnNames='%s'", sql, Arrays.toString(columnNames)));
        return execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        LOGGER.info(String.format("unwrap iface='%s'", iface));
        throw new SQLFeatureNotSupportedException("Statement.unwrap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        LOGGER.info(String.format("isWrapperFor iface='%s'", iface));
        throw new SQLFeatureNotSupportedException("Statement.isWrapperFor is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        LOGGER.info(String.format("executeUpdate sql='%s'", sql));
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate() throws SQLException {
        LOGGER.info("executeUpdate");
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOGGER.info("close");
        this.isClosed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxFieldSize() throws SQLException {
        LOGGER.info("getMaxFieldSize");
        throw new SQLFeatureNotSupportedException("Statement.getMaxFieldSize is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        LOGGER.info(String.format("setMaxFieldSize max='%d'", max));
        throw new SQLFeatureNotSupportedException("Statement.setMaxFieldSize is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxRows() {
        LOGGER.info("getMaxRows");
        return this.maxRows;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxRows(int max) {
        LOGGER.info(String.format("setMaxRows max='%d'", max));
        this.maxRows = max;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        LOGGER.info(String.format("setEscapeProcessing enable='%s'", enable));
        throw new SQLFeatureNotSupportedException("Statement.setEscapeProcessing is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        LOGGER.info("getQueryTimeout");
        return this.queryTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        LOGGER.info(String.format("setQueryTimeout seconds='%d'", seconds));
        this.queryTimeout = seconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel() throws SQLException {
        LOGGER.info("cancel");
        throw new SQLFeatureNotSupportedException("Statement.cancel is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() {
        LOGGER.info("getWarnings");
        return this.afmConnection.getWarnings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() {
        LOGGER.info("clearWarnings");
        this.afmConnection.clearWarnings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCursorName(String name) throws SQLException {
        LOGGER.info(String.format("setCursorName name='%s'", name));
        throw new SQLFeatureNotSupportedException("Statement.setCursorName is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpdateCount() {
        LOGGER.info("getUpdateCount");
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults() {
        LOGGER.info("getMoreResults");
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchDirection() {
        LOGGER.info("getFetchDirection");
        return AfmResultSet.FETCH_DIRECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        LOGGER.info(String.format("setFetchDirection direction='%d'", direction));
        throw new SQLFeatureNotSupportedException("Statement.setFetchDirection is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchSize() {
        LOGGER.info("getFetchSize");
        return this.fetchSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchSize(int rows) {
        LOGGER.info(String.format("setFetchSize rows='%d'", rows));
        this.fetchSize = rows;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetConcurrency() {
        LOGGER.info("getResultSetConcurrency");
        return AfmResultSet.CONCURRENCY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetType() {
        LOGGER.info("getResultSetType");
        return AfmResultSet.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBatch(String sql) throws SQLException {
        LOGGER.info(String.format("addBatch sql='%s'", sql));
        throw new SQLFeatureNotSupportedException("Statement.addBatch is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearBatch() throws SQLException {
        LOGGER.info("clearBatch");
        throw new SQLFeatureNotSupportedException("Statement.clearBatch is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] executeBatch() throws SQLException {
        LOGGER.info("executeBatch");
        throw new SQLFeatureNotSupportedException("Statement.executeBatch is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AfmConnection getConnection() {
        LOGGER.info("getConnection");
        return this.afmConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults(int current) {
        LOGGER.info(String.format("getMoreResults current='%d'", current));
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        LOGGER.info("getGeneratedKeys");
        throw new SQLFeatureNotSupportedException("Statement.getGeneratedKeys is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        LOGGER.info(String.format("executeUpdate sql='%s', autoGeneratedKeys='%d'", sql, autoGeneratedKeys));
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        LOGGER.info(String.format("executeUpdate sql='%s', columnIndexes='%s'", sql, Arrays.toString(columnIndexes)));
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        LOGGER.info(String.format("executeUpdate sql='%s', columnNames='%s'", sql, Arrays.toString(columnNames)));
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetHoldability() {
        LOGGER.info("getResultSetHoldability");
        return AbstractResultSet.HOLDABILITY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        LOGGER.info("isClosed");
        return this.isClosed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPoolable() {
        LOGGER.info("isPoolable");
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        LOGGER.info(String.format("setPoolable poolable='%s'", poolable));
        throw new SQLFeatureNotSupportedException("Statement.setPoolable is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeOnCompletion() {
        LOGGER.info("closeOnCompletion");
        this.isClosed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCloseOnCompletion() {
        LOGGER.info("isCloseOnCompletion");
        return this.isClosed;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        LOGGER.info(String.format("setNull parameterIndex='%d', sqlType='%d'", parameterIndex, sqlType));
        throw new SQLFeatureNotSupportedException("Statement.setNull is not supported yet.");
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        LOGGER.info("executeLargeUpdate");
        return executeUpdate();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        LOGGER.info(String.format("setBoolean parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        LOGGER.info(String.format("setByte parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        LOGGER.info(String.format("setShort parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        LOGGER.info(String.format("setInt parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        LOGGER.info(String.format("setLong parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        LOGGER.info(String.format("setFloat parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        LOGGER.info(String.format("setDouble parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        LOGGER.info(String.format("setBigDecimal parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        LOGGER.info(String.format("setString parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        LOGGER.info(String.format("setBytes parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        LOGGER.info(String.format("setDate parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        LOGGER.info(String.format("setTime parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        LOGGER.info(String.format("setTimestamp parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        LOGGER.info(String.format("setAsciiStream"));
        throw new SQLFeatureNotSupportedException("Statement.setAsciiStream is not supported yet.");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        LOGGER.info(String.format("setUnicodeStream"));
        throw new SQLFeatureNotSupportedException("Statement.setUnicodeStream is not supported yet.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        LOGGER.info(String.format("setBinaryStream"));
        throw new SQLFeatureNotSupportedException("Statement.setBinaryStream is not supported yet.");
    }

    @Override
    public void clearParameters() throws SQLException {
        this.preparedStatementParams.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        LOGGER.info(String.format("setObject parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        this.addBatch(this.sql);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        LOGGER.info(String.format("setCharacterStream"));
        throw new SQLFeatureNotSupportedException("Statement.setCharacterStream is not supported yet.");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        LOGGER.info(String.format("setRef parameterIndex='%d' x='%s'", parameterIndex, x));
        throw new SQLFeatureNotSupportedException("Statement.setRef is not supported yet.");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        LOGGER.info(String.format("setBlob parameterIndex='%d' x='%s'", parameterIndex, x));
        throw new SQLFeatureNotSupportedException("Statement.setBlob is not supported yet.");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        LOGGER.info(String.format("setClob parameterIndex='%d' x='%s'", parameterIndex, x));
        throw new SQLFeatureNotSupportedException("Statement.setClob is not supported yet.");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        LOGGER.info(String.format("setArray parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return (ResultSetMetaData) this.metadata;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        LOGGER.info(String.format("setNull parameterIndex='%d' typeName='%s'", parameterIndex, typeName));
        this.preparedStatementParams.put(parameterIndex, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        LOGGER.info(String.format("setURL parameterIndex='%d' x='%s'", parameterIndex, x));
        this.preparedStatementParams.put(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        LOGGER.info(String.format("getParameterMetaData"));
        throw new SQLFeatureNotSupportedException("Statement.getParameterMetaData is not supported yet.");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        LOGGER.info(String.format("setRowId"));
        throw new SQLFeatureNotSupportedException("Statement.setRowId is not supported yet.");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        LOGGER.info(String.format("setNCharacterStream"));
        throw new SQLFeatureNotSupportedException("Statement.setNCharacterStream is not supported yet.");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        LOGGER.info(String.format("setNClob parameterIndex='%d' value='%s'", parameterIndex, value));
        throw new SQLFeatureNotSupportedException("Statement.setNClob is not supported yet.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        LOGGER.info(String.format("setNClob parameterIndex='%d' reader='%s'", parameterIndex, reader));
        throw new SQLFeatureNotSupportedException("Statement.setClob is not supported yet.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        LOGGER.info(String.format("setBlob parameterIndex='%d' inputStream='%s'", parameterIndex, inputStream));
        throw new SQLFeatureNotSupportedException("Statement.setBlob is not supported yet.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        LOGGER.info(String.format("setNClob parameterIndex='%d' reader='%s'", parameterIndex, reader));
        throw new SQLFeatureNotSupportedException("Statement.setNClob is not supported yet.");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        LOGGER.info(String.format("setSQLXML parameterIndex='%d' xmlObject='%s'", parameterIndex, xmlObject));
        throw new SQLFeatureNotSupportedException("Statement.setSQLXML is not supported yet.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.setObject(parameterIndex, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        LOGGER.info(String.format("setAsciiStream parameterIndex='%d' x='%s'", parameterIndex, x));
        throw new SQLFeatureNotSupportedException("Statement.setAsciiStream is not supported yet.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        LOGGER.info(String.format("setBinaryStream parameterIndex='%d' x='%s'", parameterIndex, x));
        throw new SQLFeatureNotSupportedException("Statement.setBinaryStream is not supported yet.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        LOGGER.info(String.format("setCharacterStream parameterIndex='%d' reader='%s'", parameterIndex, reader));
        throw new SQLFeatureNotSupportedException("Statement.setCharacterStream is not supported yet.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        LOGGER.info(String.format("setNCharacterStream parameterIndex='%d' value='%s'", parameterIndex, value));
        throw new SQLFeatureNotSupportedException("Statement.setNCharacterStream is not supported yet.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        LOGGER.info(String.format("setClob parameterIndex='%d' reader='%s'", parameterIndex, reader));
        throw new SQLFeatureNotSupportedException("Statement.setClob is not supported yet.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        LOGGER.info(String.format("setBlob parameterIndex='%d' inputStream='%s'", parameterIndex, inputStream));
        throw new SQLFeatureNotSupportedException("Statement.setBlob is not supported yet.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        LOGGER.info(String.format("setNClob parameterIndex='%d' reader='%s'", parameterIndex, reader));
        throw new SQLFeatureNotSupportedException("Statement.setNClob is not supported yet.");
    }
}
