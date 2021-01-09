package com.gooddata.jdbc.driver;

import com.gooddata.sdk.model.executeafm.Execution;
import com.gooddata.sdk.model.executeafm.ObjQualifier;
import com.gooddata.sdk.model.executeafm.UriObjQualifier;
import com.gooddata.sdk.model.executeafm.afm.*;
import com.gooddata.sdk.model.executeafm.response.ExecutionResponse;
import com.gooddata.sdk.model.executeafm.result.ExecutionResult;
import com.gooddata.sdk.model.md.Attribute;
import com.gooddata.sdk.model.md.DisplayForm;
import com.gooddata.sdk.model.md.Entry;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.FutureResult;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.executeafm.ExecuteAfmService;
import com.gooddata.sdk.service.md.MetadataService;
import net.sf.jsqlparser.JSQLParserException;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Statement implements java.sql.Statement {
	private final static Logger logger = Logger.getGlobal();//Logger.getLogger(Statement.class.getName());

	private final Project workspace;
	private final ExecuteAfmService gdAfm;
	private final MetadataService gdMeta;
	private final Connection connection;

    private boolean isClosed = false;
	private ResultSet resultSet;
    private int maxRows = 0;

	/**
	 * Catalog of LDM objects (attributes and metrics)
	 */
	private final Map<String, CatalogEntry> catalog = new HashMap<>();

	/**
	 *
	 */
	public static class CatalogEntry {

		/**
		 * Constructor
		 * @param uri LDM object URI
		 * @param title LDM object title
		 * @param type LDM object type
		 * @param identifier LDM object identifier
		 */
		public CatalogEntry(String uri, String title, String type, String identifier, ObjQualifier ldmObject) {
			this.uri = uri;
			this.title = title;
			this.type = type;
			this.identifier = identifier;
			this.ldmObject = ldmObject;
		}

		public String getUri() {
			return uri;
		}

		public void setUri(String uri) {
			this.uri = uri;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getIdentifier() {
			return identifier;
		}

		public void setIdentifier(String identifier) {
			this.identifier = identifier;
		}

		public ObjQualifier getLdmObject() {
			return ldmObject;
		}

		public void setLdmObject(ObjQualifier ldmObject) {
			this.ldmObject = ldmObject;
		}

		private String identifier;
		private String uri;
		private String title;
		private String type;

		private ObjQualifier ldmObject;
	}

	/**
	 * Constructor
	 * @param gd GoodData connection class
	 * @param con SQL connection
	 */
	public Statement(GoodData gd, Connection con) {
		logger.info("jdbc4gd: statement constructor");
		this.gdAfm = gd.getExecuteAfmService();
		this.gdMeta = gd.getMetadataService();
		this.connection = con;
		this.workspace = con.getWorkspace();
		this.populateCatalog();
	}

	/**
	 * Populates the catalog of attributes and metrics
	 */
	private void populateCatalog() {
		Collection<Entry> metricEntries = this.gdMeta.find(this.workspace, Metric.class);

		for(Entry metric: metricEntries) {
			this.catalog.put(metric.getUri(), new CatalogEntry(metric.getUri(),
					metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
					new UriObjQualifier(metric.getUri())));
		}

		Collection<Entry> attributeEntries = this.gdMeta.find(this.workspace, Attribute.class);
		for(Entry attribute: attributeEntries) {
			Attribute a = this.gdMeta.getObjByUri(attribute.getUri(), Attribute.class);
			DisplayForm displayForm = a.getDefaultDisplayForm();
			//TODO getting default display form under the attribute title
			this.catalog.put(displayForm.getUri(), new CatalogEntry(displayForm.getUri(),
					a.getTitle(), displayForm.getCategory(), displayForm.getIdentifier(),
					new UriObjQualifier(displayForm.getUri())));
		}
	}

	/**
	 * Duplicate LDM object exception is thrown when there are multiple LDM objects with the same title
	 */
	public static class DuplicateLdmObjectException extends Exception {
		public DuplicateLdmObjectException(String e) {
			super(e);
		}
	}

	/**
	 * Thrown when a LDM object with a title isn't found
	 */
	public static class LdmObjectNotFoundException extends Exception {
		public LdmObjectNotFoundException(String e) {
			super(e);
		}
	}

	private CatalogEntry findObjectByName(String name) throws DuplicateLdmObjectException,
			LdmObjectNotFoundException {
		List<CatalogEntry> objects = this.catalog.values().stream()
				.filter(catalogEntry -> name.equalsIgnoreCase(catalogEntry.getTitle())).collect(Collectors.toList());
		if(objects.size() > 1) {
			throw new DuplicateLdmObjectException(
					String.format("Column name '%s' can't be uniquely resolved. " +
							"There are multiple LDM objects with this title.", name));
		} else if(objects.size() == 0) {
			throw new LdmObjectNotFoundException(
					String.format("Column name '%s' doesn't exist.", name));
		}
		return objects.get(0);
	}

	private List<CatalogEntry> resolveColumns(SQLParser.ParsedSQL sql) throws DuplicateLdmObjectException,
			LdmObjectNotFoundException {
		List<CatalogEntry> c = new ArrayList<>();
		List<String> columns = sql.getColumns();
		for(String name: columns ) {
			c.add(findObjectByName(name));
		}
		return c;
	}

	/**
	 * Populates AFM execution parameter
	 * @param columns resolved SQL columns
	 * @return AFM object
	 * @throws DuplicateLdmObjectException when there are multiple LDM object with a named mentioned in the parsed SQL
	 * @throws LdmObjectNotFoundException when a SQL object (column) can't be resolved
	 */
	private Afm getAfm(List<CatalogEntry> columns) throws DuplicateLdmObjectException,
			LdmObjectNotFoundException {
		Afm afm = new Afm();
		for( CatalogEntry o: columns ) {
			if(o.getType().equalsIgnoreCase("attributeDisplayForm")) {
				afm.addAttribute(new AttributeItem(o.getLdmObject(), o.getIdentifier()));
			} else if(o.getType().equalsIgnoreCase("metric")) {
				afm.addMeasure(new MeasureItem( new SimpleMeasureDefinition(o.getLdmObject()),
						o.getIdentifier()));
			}
		}
		return afm;
	}

	/**
	 * Executes SQL query
	 * @param sql SQL query (only SELECTs are supported)
	 * @return ResultSet
	 * @throws SQLException in case of execution problems
	 */
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
			logger.info("jdbc4gd: statement executeQuery sql:"+sql);
		try {
			SQLParser parser = new SQLParser();
			SQLParser.ParsedSQL parsedSql = parser.parse(sql);
			List<CatalogEntry> columns = this.resolveColumns(parsedSql);
			Afm afm = getAfm(columns);
			ExecutionResponse rs = this.gdAfm.executeAfm(this.workspace, new Execution(afm));
			FutureResult<ExecutionResult> fr = this.gdAfm.getResult(rs);
			return new ResultSetTable(this, fr.get(), columns);
		} catch (JSQLParserException | LdmObjectNotFoundException | DuplicateLdmObjectException e) {
			throw new SQLException(e);
		}
	}

	/**
	 * Executes SQL query
	 * @param sql SQL query (only SELECTs are supported)
	 * @return true in case of success
	 * @throws SQLException in case of execution problems
	 */
	@Override
	public boolean execute(String sql) throws SQLException {
		this.resultSet = this.executeQuery(sql);
		return true;
	}

	@Override
	public java.sql.ResultSet getResultSet()  {
		return this.resultSet;
	}

	/**
	 * Executes SQL query
	 * @param sql SQL query
	 * @param autoGeneratedKeys ignored
	 * @return true in case when everything is fine
	 * @throws SQLException in case of execution problems
	 */
	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return execute(sql);
	}

	/**
	 * Executes SQL query
	 * @param sql SQL query
	 * @param columnIndexes ignored
	 * @return true in case when everything is fine
	 * @throws SQLException in case of execution problems
	 */
	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return execute(sql);
	}
	/**
	 * Executes SQL query
	 * @param sql SQL query
	 * @param columnNames ignored
	 * @return true in case when everything is fine
	 * @throws SQLException in case of execution problems
	 */
	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return execute(sql);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}


	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void close() {
        this.isClosed=true;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getMaxRows() {
		return this.maxRows;
	}

	@Override
	public void setMaxRows(int max){
        this.maxRows=max;
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void cancel() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getUpdateCount() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getResultSetType() {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public Connection getConnection() {
		return this.connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public int getResultSetHoldability() {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public boolean isClosed() {
		return this.isClosed;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	@Override
	public void closeOnCompletion() {
		this.isClosed = true;
	}

	@Override
	public boolean isCloseOnCompletion() {
		return this.isClosed;
	}

}
