package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.catalog.AfmFilter;
import com.gooddata.jdbc.catalog.Catalog;
import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.metadata.AfmDatabaseMetaData;
import com.gooddata.jdbc.parser.MaqlParser;
import com.gooddata.jdbc.parser.SQLParser;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.logging.Logger;

/**
 * JDBC statement
 */
public class AfmStatement implements java.sql.Statement {

    private final static Logger LOGGER = Logger.getLogger(AfmStatement.class.getName());

    private final Project workspace;
    private final AfmConnection afmConnection;
    private final AfmDatabaseMetaData metadata;
    private final ExecuteAfmService gdAfm;
    private final MetadataService gdMeta;

    private boolean isClosed = false;
    private AfmResultSet resultSet;
    private int maxRows = 0;

    private int fetchSize = 0;


    /**
     * Constructor
     *
     * @param con      java.sql.Connection
     * @param gd       GoodData connection class
     * @param metadata database metadata
     */
    public AfmStatement(AfmConnection con, GoodData gd, AfmDatabaseMetaData metadata) {
        this.afmConnection = con;
        this.metadata = metadata;
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
        try {
            SQLParser parser = new SQLParser();
            SQLParser.ParsedSQL parsedSql = parser.parseQuery(sql);
            List<CatalogEntry> columns = this.metadata.getCatalog().resolveAfmColumns(parsedSql);
            List<AfmFilter> filters = this.metadata.getCatalog().resolveAfmFilters(parsedSql);
            List<SortItem> orderBys = this.metadata.getCatalog().resolveOrderBys(parsedSql, columns);
            Afm afm = getAfm(columns, filters);
            return  new AfmResultSet(this, this.workspace, this.gdAfm, afm, columns, orderBys,
                    parsedSql.getLimit(), parsedSql.getOffset());
        } catch (JSQLParserException | Catalog.CatalogEntryNotFoundException
                | Catalog.DuplicateCatalogEntryException | TextUtil.InvalidFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql) throws SQLException {
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
     * @throws TextUtil.InvalidFormatException invalid format of URI
     */
    public void executeCreateMetric(MaqlParser.ParsedCreateMetricStatement parsedMaqlCreate) throws
            Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException,
            TextUtil.InvalidFormatException {

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
        CatalogEntry ldmObj = this.metadata.getCatalog().findAfmColumn(metricName);
        this.gdMeta.removeObjByUri(ldmObj.getUri());
        this.metadata.getCatalog().remove(ldmObj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.ResultSet getResultSet() {
        return this.resultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.unwrap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.isWrapperFor is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.isClosed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getMaxFieldSize is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.setMaxFieldSize is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxRows() {
        return this.maxRows;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxRows(int max) {
        this.maxRows = max;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.setEscapeProcessing is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getQueryTimeout is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.setQueryTimeout is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.cancel is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getWarnings is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.clearWarnings is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.setCursorName is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpdateCount() {
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.setFetchDirection is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getFetchDirection is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchSize(int rows) {
        this.fetchSize = rows;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchSize() {
        return this.fetchSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getResultSetConcurrency is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getResultSetType is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.addBatch is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.clearBatch is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.executeBatch is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AfmConnection getConnection() {
        return this.afmConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getGeneratedKeys is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.executeUpdate is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.getResultSetHoldability is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.setPoolable is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement.isPoolable is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeOnCompletion() {
        this.isClosed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCloseOnCompletion() {
        return this.isClosed;
    }

}
