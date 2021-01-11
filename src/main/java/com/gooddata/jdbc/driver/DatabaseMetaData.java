package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.MetadataResultSet;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.executeafm.ObjQualifier;
import com.gooddata.sdk.model.executeafm.UriObjQualifier;
import com.gooddata.sdk.model.md.Attribute;
import com.gooddata.sdk.model.md.DisplayForm;
import com.gooddata.sdk.model.md.Entry;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.md.MetadataService;

import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DatabaseMetaData implements java.sql.DatabaseMetaData {

    private final static Logger LOGGER = Logger.getLogger(DatabaseMetaData.class.getName());

    private final com.gooddata.jdbc.driver.Connection connection;
    private final Project workspace;
    private final MetadataService gdMeta;
    private final String user;

    /**
     * Catalog of LDM objects (attributes and metrics)
     */
    private final Map<String, CatalogEntry> catalog = new HashMap<>();

    public DatabaseMetaData(com.gooddata.jdbc.driver.Connection connection, GoodData gd,
                            Project workspace, String user) throws SQLException {
        this.connection = connection;
        this.gdMeta = gd.getMetadataService();
        this.workspace = workspace;
        this.user = user;
        this.populateCatalog();
    }

    private MetadataResultSet populateCatalogResultSet() {
        return new MetadataResultSet(
                Collections.singletonList(
                        new MetadataResultSet.MetaDataColumn("TABLE_CAT",
                                Collections.singletonList(""))
                )
        );
    }

    private MetadataResultSet populateTableTypeResultSet() {
        return new MetadataResultSet(
                Collections.singletonList(
                        new MetadataResultSet.MetaDataColumn("TABLE_TYPE",
                                Arrays.asList("GLOBAL TEMPORARY", "LOCAL TEMPORARY",
                                        "SYSTEM TABLE", "TABLE", "VIEW"))
                ));
    }

    private MetadataResultSet populateEmptyResultSet() throws SQLException {
        List<String> empty = new ArrayList<>();
        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("1",
                        empty),
                new MetadataResultSet.MetaDataColumn("2",
                        empty),
                new MetadataResultSet.MetaDataColumn("3",
                        empty),
                new MetadataResultSet.MetaDataColumn("4",
                        empty),
                new MetadataResultSet.MetaDataColumn("5",
                        empty),
                new MetadataResultSet.MetaDataColumn("6",
                        empty),
                new MetadataResultSet.MetaDataColumn("7",
                        empty),
                new MetadataResultSet.MetaDataColumn("8",
                        empty),
                new MetadataResultSet.MetaDataColumn("9",
                        empty),
                new MetadataResultSet.MetaDataColumn("10",
                        empty)
        );
        return new MetadataResultSet(data);
    }

    private MetadataResultSet populateSchemaResultSet() throws SQLException {
        List<String> uniqueSchemas = this.getSchemasList();
        List<String> catalogs = uniqueSchemas.stream()
                .map(e -> "").collect(Collectors.toList());
        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                        uniqueSchemas),
                new MetadataResultSet.MetaDataColumn("TABLE_CATALOG",
                        catalogs)
        );
        return new MetadataResultSet(data);
    }

    private MetadataResultSet populateColumnResultSet() throws SQLException {

        List<String> columns = this.catalog.values().stream()
                .map(i->i.getTitle()).collect(Collectors.toList());
        List<String> nil = columns.stream()
                        .map(e -> (String)null)
                        .collect(Collectors.toList());
        List<String> empty = columns.stream()
                .map(e -> "")
                .collect(Collectors.toList());
        List<String> ordinal = IntStream.range(1, columns.size()+1)
                .boxed().map(e -> Integer.toString(e)).collect(Collectors.toList());

        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("TABLE_CAT",
                        columns.stream()
                                .map(e -> "")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                        columns.stream()
                                .map(e -> this.workspace.getId())
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("TABLE_NAME",
                        columns.stream()
                                .map(e -> ResultSetTableMetaData.UNIVERSAL_TABLE_NAME)
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("COLUMN_NAME",
                        columns),
                new MetadataResultSet.MetaDataColumn("DATA_TYPE", "INTEGER",
                        this.catalog.values().stream()
                                .map(e -> e.getType().equals("metric")?
                                        Integer.toString(java.sql.Types.NUMERIC)
                                        : Integer.toString(java.sql.Types.VARCHAR))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("TYPE_NAME",
                        this.catalog.values().stream()
                                .map(e -> e.getType().equals("metric") ?
                                        new String("Numeric")
                                        : new String("Varchar"))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("COLUMN_SIZE",  "INTEGER",
                        this.catalog.values().stream()
                                .map(e -> e.getType().equals("metric") ?
                                        new String("15")
                                        : new String("255"))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("BUFFER_LENGTH", "INTEGER",
                        this.catalog.values().stream()
                        .map(e -> e.getType().equals("metric") ?
                                new String("15")
                                : new String("255"))
                        .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("DECIMAL_DIGITS", "INTEGER",
                        this.catalog.values().stream()
                                .map(e -> e.getType().equals("metric") ?
                                        new String("2") : (String)null)
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("NUM_PREC_RADIX", "INTEGER",
                        this.catalog.values().stream()
                                .map(e -> e.getType().equals("metric") ?
                                        new String("10") : (String)null)
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("NULLABLE", "INTEGER",
                        columns.stream()
                                .map(e -> "1")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("REMARKS", nil) ,
                new MetadataResultSet.MetaDataColumn("COLUMN_DEF", empty),
                new MetadataResultSet.MetaDataColumn("SQL_DATA_TYPE",  "INTEGER",
                        this.catalog.values().stream()
                            .map(e -> e.getType().equals("metric")?
                                Integer.toString(java.sql.Types.NUMERIC)
                                : Integer.toString(java.sql.Types.VARCHAR))
                            .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("SQL_DATETIME_SUB",  "INTEGER", nil),
                new MetadataResultSet.MetaDataColumn("CHAR_OCTET_LENGTH", "INTEGER",
                        this.catalog.values().stream()
                        .map(e -> e.getType().equals("metric") ?
                                (String)null
                                : new String("255"))
                        .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("ORDINAL_POSITION",  "INTEGER", ordinal),
                new MetadataResultSet.MetaDataColumn("IS_NULLABLE",
                        columns.stream()
                                .map(e -> "YES")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("SCOPE_CATALOG", nil),
                new MetadataResultSet.MetaDataColumn("SCOPE_SCHEMA", nil),
                new MetadataResultSet.MetaDataColumn("SCOPE_TABLE", nil),
                new MetadataResultSet.MetaDataColumn("SOURCE_DATA_TYPE", nil),
                new MetadataResultSet.MetaDataColumn("IS_AUTOINCREMENT",
                        columns.stream()
                                .map(e -> "NO")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("IS_GENERATEDCOLUMN",
                        columns.stream()
                                .map(e -> "NO")
                                .collect(Collectors.toList()))
        );
        return new MetadataResultSet(data);
    }

    private MetadataResultSet populateTableResultSet() throws SQLException {
        List<String> empty = Collections.nCopies(1, (String) null);
        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("TABLE_CAT",
                        Arrays.asList("")),
                new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                        Arrays.asList(this.workspace.getId())),
                new MetadataResultSet.MetaDataColumn("TABLE_NAME",
                        Arrays.asList(ResultSetTableMetaData.UNIVERSAL_TABLE_NAME)),
                new MetadataResultSet.MetaDataColumn("TABLE_TYPE",
                        Arrays.asList("TABLE")),
                new MetadataResultSet.MetaDataColumn("REMARKS", empty)
                        ,
                new MetadataResultSet.MetaDataColumn("TYPE_CAT",
                        empty),
                new MetadataResultSet.MetaDataColumn("TYPE_SCHEM",
                        empty),
                new MetadataResultSet.MetaDataColumn("TYPE_NAME",
                        empty),
                new MetadataResultSet.MetaDataColumn("SELF_REFERENCING_COL_NAME",
                        empty),
                new MetadataResultSet.MetaDataColumn("REF_GENERATION",
                        empty)
        );
        return new MetadataResultSet(data);
    }

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

    public int getCatalogRowCount() {
        return this.catalog.size();
    }


    List<String> LDM = Arrays.asList("Quarter/Year (Date)", "Product Category",
            "Product","Revenue", "# of Orders");

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

    protected List<CatalogEntry> resolveColumns(SQLParser.ParsedSQL sql) throws DuplicateLdmObjectException,
            LdmObjectNotFoundException {
        List<CatalogEntry> c = new ArrayList<>();
        List<String> columns = sql.getColumns();
        for(String name: columns ) {
            c.add(findObjectByName(name));
        }
        return c;
    }

    public String getUser() {
        return this.user;
    }

    public Project getWorkspace() {
        return this.workspace;
    }

    public String getWorkspaceId() {
        return this.workspace.getId();
    }

    public String getWorkspaceUri() {
        return this.workspace.getUri();
    }

    @Override
    public boolean allProceduresAreCallable()  {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable()  {
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getURL is not implemented yet");
    }

    @Override
    public String getUserName()  {
        return this.user;
    }

    @Override
    public boolean isReadOnly()  {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh()  {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow()  {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart()  {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd()  {
        return true;
    }

    @Override
    public String getDatabaseProductName()  {
        return "GoodData";
    }

    @Override
    public String getDatabaseProductVersion()  {
        return Driver.VERSION;
    }

    @Override
    public String getDriverName()  {
        return "gooddata-jdbc";
    }

    @Override
    public String getDriverVersion()  {
        return Driver.VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return Driver.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return Driver.MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles()  {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()  {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()  {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()  {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()  {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()  {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()  {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()  {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()  {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()  {
        return false;
    }

    @Override
    public String getIdentifierQuoteString()  {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSQLKeywords is not implemented yet");
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getNumericFunctions is not implemented yet");
    }

    @Override
    public String getStringFunctions()  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getStringFunctions is not implemented yet");
    }

    @Override
    public String getSystemFunctions()  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSystemFunctions is not implemented yet");
    }

    @Override
    public String getTimeDateFunctions()  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getTimeDateFunctions is not implemented yet");
    }

    @Override
    public String getSearchStringEscape()  {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters()  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getExtraNameCharacters is not implemented yet");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()  {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()  {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()  {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull()  {
        return false;
    }

    @Override
    public boolean supportsConvert()  {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType)  {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()  {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()  {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()  {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated()  {
        return false;
    }

    @Override
    public boolean supportsGroupBy()  {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated()  {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()  {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause()  {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets()  {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()  {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns()  {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()  {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar()  {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()  {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()  {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()  {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()  {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()  {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()  {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins()  {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins()  {
        return false;
    }

    @Override
    public String getSchemaTerm()  throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm()  throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm()  throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart()  {
        return false;
    }

    @Override
    public String getCatalogSeparator()  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getCatalogSeparator is not implemented yet");
    }

    @Override
    public boolean supportsSchemasInDataManipulation()  {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()  {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()  {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()  {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()  {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()  {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()  {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()  {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()  {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()  {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete()  {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()  {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()  {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()  {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()  {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists()  {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns()  {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()  {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()  {
        return false;
    }

    @Override
    public boolean supportsUnion()  {
        return false;
    }

    @Override
    public boolean supportsUnionAll()  {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()  {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()  {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()  {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()  {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength()  throws SQLException {
        return 1024;
    }

    @Override
    public int getMaxCharLiteralLength()  throws SQLException {
        return 1024;
    }

    @Override
    public int getMaxColumnNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInGroupBy()  throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInIndex()  throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInOrderBy()  throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInSelect()  throws SQLException {
        return 32;
    }

    @Override
    public int getMaxColumnsInTable()  throws SQLException {
        return 256;
    }

    @Override
    public int getMaxConnections()  throws SQLException {
        return 16;
    }

    @Override
    public int getMaxCursorNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxIndexLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxSchemaNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxProcedureNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxCatalogNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxRowSize()  throws SQLException {
        return 16384;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()  {
        return true;
    }

    @Override
    public int getMaxStatementLength()  throws SQLException {
        return 16384;
    }

    @Override
    public int getMaxStatements()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxTableNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getMaxTablesInSelect()  throws SQLException {
        return 16;
    }

    @Override
    public int getMaxUserNameLength()  throws SQLException {
        return 64;
    }

    @Override
    public int getDefaultTransactionIsolation()  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getDefaultTransactionIsolation is not implemented yet");
    }

    @Override
    public boolean supportsTransactions()  {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)  {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()  {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()  {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()  {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()  {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern)    throws SQLException {
        return this.populateEmptyResultSet();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern,
                                         String columnNamePattern)    throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getProcedureColumns is not implemented yet");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {
        //TODO filter the resultset
        return this.populateTableResultSet();
    }

    @Override
    public ResultSet getSchemas()  throws SQLException {
        return this.populateSchemaResultSet();
    }

    private List<String> getSchemasList() throws SQLException {
        Set<String> schemas = new HashSet<>();
        for(String uri: this.catalog.keySet()) {
            schemas.add(TextUtil.extractWorkspaceIdFromUri(uri));
        }
        return new ArrayList<>(schemas);
    }

    @Override
    public ResultSet getCatalogs()  throws SQLException {
        return this.populateCatalogResultSet();
    }

    @Override
    public ResultSet getTableTypes()  throws SQLException {
        return this.populateTableTypeResultSet();
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern)  throws SQLException {
        //TODO filter the resultset
        return this.populateColumnResultSet();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getColumnPrivileges is not implemented yet");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern)  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getTablePrivileges is not implemented yet");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
                                          int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getBestRowIdentifier is not implemented yet");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema,
                                       String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getVersionColumns is not implemented yet");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
        return this.populateEmptyResultSet();
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
        return this.populateEmptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getExportedKeys is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                       String parentTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getCrossReference is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getTypeInfo is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema,
                                  String table, boolean unique,
                                  boolean approximate) throws SQLException {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
        return this.populateEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetType(int type)  {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)  {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type)  {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type)  {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type)  {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type)  {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type)  {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type)  {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type)  {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type)  {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type)  {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()  {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern, int[] types)  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getUDTs is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public Connection getConnection()  {
        return this.connection;
    }

    @Override
    public boolean supportsSavepoints()  {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()  {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults()  {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()  {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                   String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSuperTypes is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern,
                                    String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSuperTables is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern,
                                   String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getAttributes is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability)  {
        return false;
    }

    @Override
    public int getResultSetHoldability()  {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion()  {
        return Driver.MAJOR_VERSION;
    }

    @Override
    public int getDatabaseMinorVersion()  {
        return Driver.MINOR_VERSION;
    }

    @Override
    public int getJDBCMajorVersion()  {
        return Driver.MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion()  {
        return Driver.MINOR_VERSION;
    }

    @Override
    public int getSQLStateType()  {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy()  {
        return false;
    }

    @Override
    public boolean supportsStatementPooling()  {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getRowIdLifetime is not implemented yet");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        //TODO filter the resultset
        return this.populateEmptyResultSet();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()  {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()  {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()  throws SQLException{
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getClientInfoProperties is not implemented yet");
        //return this.emptyResultSet;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        //throw new SQLFeatureNotSupportedException("Not supported yet");
        return this.populateEmptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getFunctionColumns is not implemented yet");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                      String tableNamePattern,
                                      String columnNamePattern)  throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getPseudoColumns is not implemented yet");
    }

    @Override
    public boolean generatedKeyAlwaysReturned()  {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.unwrap is not implemented yet");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.isWrapperFor is not implemented yet");
    }
}
