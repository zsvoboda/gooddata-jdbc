package com.gooddata.jdbc.driver;

import com.gooddata.sdk.model.executeafm.Execution;
import com.gooddata.sdk.model.executeafm.afm.Afm;
import com.gooddata.sdk.model.executeafm.afm.AttributeItem;
import com.gooddata.sdk.model.executeafm.afm.MeasureItem;
import com.gooddata.sdk.model.executeafm.afm.SimpleMeasureDefinition;
import com.gooddata.sdk.model.executeafm.response.ExecutionResponse;
import com.gooddata.sdk.model.executeafm.result.ExecutionResult;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.FutureResult;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.executeafm.ExecuteAfmService;
import com.gooddata.sdk.service.md.MetadataService;
import net.sf.jsqlparser.JSQLParserException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
	private ResultSet resultSet;
    private int maxRows = 0;

    private int fetchSize = 0;


	/**
	 * Constructor
	 * @param con java.sql.Connection
	 * @param gd GoodData connection class
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
	 * @param columns AFM columns
	 * @param filters AFM filters
	 * @return AFM object
	 * @throws Catalog.DuplicateCatalogEntryException when there are multiple LDM object with a named mentioned in the parsed SQL
	 * @throws Catalog.CatalogEntryNotFoundException when a SQL object (column) can't be resolved
	 */
	private Afm getAfm(List<CatalogEntry> columns, List<AfmFilter> filters) throws Catalog.DuplicateCatalogEntryException,
			Catalog.CatalogEntryNotFoundException {
		Afm afm = new Afm();
		for( CatalogEntry o: columns ) {
			if(o.getType().equalsIgnoreCase("attributeDisplayForm")) {
				afm.addAttribute(new AttributeItem(o.getGdObject(), o.getIdentifier()));
			} else if(o.getType().equalsIgnoreCase("metric")) {
				afm.addMeasure(new MeasureItem( new SimpleMeasureDefinition(o.getGdObject()),
						o.getIdentifier()));
			}
		}
		for( AfmFilter f: filters ) {
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
			SQLParser.ParsedSQL parsedSql = parser.parse(sql);
			List<CatalogEntry> columns = this.metadata.getCatalog().resolveAfmColumns(parsedSql);
			List<AfmFilter> filters = this.metadata.getCatalog().resolveAfmFilters(parsedSql);
			Afm afm = getAfm(columns, filters);
			ExecutionResponse rs = this.gdAfm.executeAfm(this.workspace, new Execution(afm));
			FutureResult<ExecutionResult> fr = this.gdAfm.getResult(rs);
			return new AfmResultSet(this, fr.get(), columns);
		} catch (JSQLParserException | Catalog.CatalogEntryNotFoundException
				| Catalog.DuplicateCatalogEntryException e) {
			throw new SQLException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean execute(String sql) throws SQLException {
		if(sql.trim().toLowerCase().startsWith("create")) {
			try {
				SQLParser parser = new SQLParser();
				SQLParser.ParsedCreateMetricStatement parsedCreate
						= parser.parseCreateOrAlterMetric(sql);
				this.executeCreateMetric(parsedCreate);
			} catch (Catalog.CatalogEntryNotFoundException |
					Catalog.DuplicateCatalogEntryException
					| JSQLParserException e) {
				throw new SQLException(e);
			}
		} else if(sql.trim().toLowerCase().startsWith("alter")) {
			try {
				SQLParser parser = new SQLParser();
				SQLParser.ParsedCreateMetricStatement parsedCreate
						= parser.parseCreateOrAlterMetric(sql);
				this.executeAlterMetric(parsedCreate);
			} catch (Catalog.CatalogEntryNotFoundException |
					Catalog.DuplicateCatalogEntryException
					| JSQLParserException e) {
				throw new SQLException(e);
			}
		}
		else if(sql.trim().toLowerCase().startsWith("drop")) {
			try {
				SQLParser parser = new SQLParser();
				String parsedDropMetric = parser.parseDropMetric(sql);
				this.executeDropMetric(parsedDropMetric);
			} catch (Catalog.CatalogEntryNotFoundException | Catalog.DuplicateCatalogEntryException
					| JSQLParserException e) {
				throw new SQLException(e);
			}
		}
		else {
			this.resultSet = this.executeQuery(sql);
		}
		return true;
	}

	/**
	 * Execute CREATE METRIC statement
	 * @param sql CREATE METRIC statement
	 * @throws Catalog.CatalogEntryNotFoundException issues with resolving referenced objects
	 * @throws Catalog.DuplicateCatalogEntryException issues with resolving referenced objects
	 */
	public void executeCreateMetric(SQLParser.ParsedCreateMetricStatement sql) throws
			Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException, SQLException {
		String maqlDefinition = sql.getMetricMaqlDefinition();
		for(String metricFactAttribute: sql.getLdmObjectTitles()) {
			//lookup attribute in LDM
			CatalogEntry ldmObj = this.metadata.getCatalog().findLdmColumnByTitle(metricFactAttribute);
			String replaceWhat = String.format("\"%s\"", metricFactAttribute);
			maqlDefinition = maqlDefinition.replaceAll(
					replaceWhat,
					String.format("[%s]", ldmObj.getUri()));
		}
		// TODO replace attribute elements

		Map<String,String> elementToAttribute = sql.getAttributeElementToAttributeNameLookup();
		for(String value: sql.getAttributeElementValues()) {
			String attributeName = elementToAttribute.get(value);
			if(attributeName == null)
					throw new Catalog.CatalogEntryNotFoundException(
							"The value '%s' can't be associated with any attribute.");
			//lookup display form in AFM
			CatalogEntry ldmObj = this.metadata.getCatalog().findAfmColumnByTitle(attributeName);
			String replaceWhat = String.format("'%s'", value);
			Map<String, String> lookup = this.metadata.getCatalog().lookupAttributeElements(ldmObj.getUri(),
					Collections.singletonList(value));
			if(lookup == null || lookup.size() == 0)
				throw new Catalog.CatalogEntryNotFoundException(
						"The value '%s' can't mapped to any element URI.");
			String elementUri = lookup.get(value);
			if(elementUri == null || elementUri.length() == 0)
				throw new Catalog.CatalogEntryNotFoundException(
						"The value '%s' doesn't exist.");
			String replaceWith = String.format("[%s]", elementUri);
			maqlDefinition = maqlDefinition.replaceAll(
					replaceWhat,
					replaceWith);
		}

		// TODO format
		Metric m = new Metric(sql.getName(),
				maqlDefinition,
				"###,###.00");
		Metric newMetric = this.gdMeta.createObj(this.workspace, m);
		this.metadata.getCatalog().addMetric(newMetric);
	}

	/**
	 * Execute ALTER METRIC statement
	 * @param sql ALTER METRIC statement
	 * @throws Catalog.CatalogEntryNotFoundException issues with resolving referenced objects
	 * @throws Catalog.DuplicateCatalogEntryException issues with resolving referenced objects
	 */
	public void executeAlterMetric(SQLParser.ParsedCreateMetricStatement sql) throws
			Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException {
		String maqlDefinition = sql.getMetricMaqlDefinition();
		for(String metricFactAttribute: sql.getLdmObjectTitles()) {
			CatalogEntry ldmObj = this.metadata.getCatalog().findLdmColumnByTitle(metricFactAttribute);
			maqlDefinition = maqlDefinition.replaceAll(metricFactAttribute, String.format("[%s]", ldmObj.getUri()));
		}
		// TODO replace attribute elements

		// TODO lookup
		CatalogEntry afmMetric = this.metadata.getCatalog()
				.findAfmColumnByTitle(sql.getName());

		Metric m = new Metric(sql.getName(),
				sql.getMetricMaqlDefinition(),
				"###,###.00");
		m.setIdentifier(afmMetric.getIdentifier());
		this.gdMeta.updateObj(m);
	}


	/**
	 * Execute DROP METRIC statement
	 * @param metricName dropped metric name
	 * @throws Catalog.CatalogEntryNotFoundException issues with resolving referenced objects
	 * @throws Catalog.DuplicateCatalogEntryException issues with resolving referenced objects
	 */
	public void executeDropMetric(String metricName) throws
			Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException {
		CatalogEntry ldmObj = this.metadata.getCatalog().findAfmColumnByTitle(metricName);
		this.gdMeta.removeObjByUri(ldmObj.getUri());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.sql.ResultSet getResultSet()  {
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
        this.isClosed=true;
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
	public void setMaxRows(int max){
        this.maxRows=max;
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
	 */@Override
	public boolean isCloseOnCompletion() {
		return this.isClosed;
	}

}
