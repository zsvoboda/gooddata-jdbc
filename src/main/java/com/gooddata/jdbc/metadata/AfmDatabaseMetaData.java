package com.gooddata.jdbc.metadata;

import com.gooddata.jdbc.catalog.Catalog;
import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.catalog.Schema;
import com.gooddata.jdbc.driver.AfmConnection;
import com.gooddata.jdbc.driver.AfmDriver;
import com.gooddata.jdbc.rest.GoodDataRestConnection;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import org.springframework.web.client.RestTemplate;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Database metadata - converts between GoodData and JDBC metadata
 */
public class AfmDatabaseMetaData implements java.sql.DatabaseMetaData {

    private final static Logger LOGGER = Logger.getLogger(AfmDatabaseMetaData.class.getName());

    // JDBC connection
    private final AfmConnection afmConnection;
    // GoodData connection
    private final GoodData gd;
    //Schemas = GD workspaces
    private final List<Schema> schemas;
    // GoodData user
    private final String user;
    // GoodData REST connection
    private GoodDataRestConnection gdRestConnection;
    // GoodData REstTemplate
    private final RestTemplate gdRestTemplate;

    //Current schema
    private Schema schema;

    /**
     * Catalog of LDM objects (attributes and metrics)
     */
    private Catalog catalog;

    /**
     * DatabaseMetadata constructor
     * @param afmConnection SQL connection
     * @param gd GoodData connection
     * @param user username
     * @param gdRestTemplate GD Spring RestTemplate for direct GD invocation
     * @throws SQLException error
     */
    public AfmDatabaseMetaData(AfmConnection afmConnection, GoodData gd, String workspaceId, String user,
                               RestTemplate gdRestTemplate) throws SQLException {
        this.afmConnection = afmConnection;
        this.gd = gd;
        this.user = user;
        this.gdRestTemplate = gdRestTemplate;
        this.schemas = Schema.populateSchemas(this.gd);
        Schema schema = findSchemaByUri(String.format("/gdc/projects/%s", workspaceId));
        this.setActiveWorkspace(schema);
        this.setSchema(schema.getSchemaName());
    }

    /**
     * Gets the GoodData objects catalog
     * @return GoodData objects catalog
     */
    public Catalog getCatalog() {
        this.catalog.waitForCatalogPopulationFinished();
        return catalog;
    }

    private Schema findSchemaByUri(String uri) throws SQLException {
        List<Schema> schemas = this.schemas.stream().filter(e->e.getSchemaUri().equals(uri))
                .collect(Collectors.toList());
        if(schemas.size() > 1)
            throw new SQLException(String.format("Duplicate schema uri '%s'", uri));
        if(schemas.size() <= 0)
            throw new SQLException(String.format("Schema with uri '%s' not found.", uri));
        return schemas.get(0);
    }

    private Schema findSchemaByName(String schemaName) throws SQLException {
        List<Schema> schemas = this.schemas.stream().filter(e->e.getSchemaName().equals(schemaName))
                .collect(Collectors.toList());
        if(schemas.size() > 1)
            throw new SQLException(String.format("Duplicate schema names '%s'", schemaName));
        if(schemas.size() <= 0)
            throw new SQLException(String.format("Schema '%s' not found.", schemaName));
        return schemas.get(0);
    }

    private Project getWorkspaceForSchema(Schema schema) {
        return this.gd.getProjectService().getProjectByUri(schema.getSchemaUri());
    }

    private void setActiveWorkspace(Schema schema) {
        this.gdRestConnection = new GoodDataRestConnection(this.gdRestTemplate,
                this.getWorkspaceForSchema(schema));
    }

    public void setSchema(String schemaName) throws SQLException {
        LOGGER.info(String.format("AfmDatabaseMetaData::setSchema '%s'", schemaName));
        this.schema = findSchemaByName(schemaName);
        this.catalog = AfmDriver.getCachedCatalog(schema.getSchemaUri());
        if(this.catalog == null) {
            LOGGER.info(String.format("Cached catalog for schema '%s' not found.", schemaName));
            Catalog tryDeserialize = new Catalog();
            try {
                LOGGER.info(String.format("Trying to deserialize catalog for '%s'.", schemaName));
                tryDeserialize.deserialize(TextUtil
                        .extractWorkspaceIdFromWorkspaceUri(schema.getSchemaUri()));
                LOGGER.info("Catalog successfully  deserialized.");
                this.catalog = tryDeserialize;
            } catch (IOException e) {
                LOGGER.info("Catalog deserialization failed. Creating new one.");
                this.catalog = new Catalog();
                LOGGER.info("Starting async catalog population.");
                this.catalog.populateAsync(gd, this.gdRestConnection, schema.getSchemaUri());
            } catch (ClassNotFoundException | TextUtil.InvalidFormatException e) {
                throw new SQLException(e);
            }
        }
        LOGGER.info("Storing catalog to cache.");
        AfmDriver.cacheCatalog(schema.getSchemaUri(), this.catalog);
        LOGGER.info("Setting active workspace.");
        this.setActiveWorkspace(schema);
    }

    public String getSchema() {
        LOGGER.info(String.format("AfmDatabaseMetaData::getSchema schema '%s'", this.schema.getSchemaName()));
        return this.schema.getSchemaName();
    }

    public GoodDataRestConnection getGoodDataRestConnection() {
        return this.gdRestConnection;
    }

    public Project getWorkspace() {
        return this.getWorkspaceForSchema(this.schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean allTablesAreSelectable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getURL() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getURL is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUserName() {
        return this.user;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nullsAreSortedAtStart() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nullsAreSortedAtEnd() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabaseProductName() {
        return "GoodData";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabaseProductVersion() {
        return AfmDriver.VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverName() {
        return "gooddata-jdbc";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDriverVersion() {
        return AfmDriver.VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getDriverMajorVersion() {
        return AfmDriver.MAJOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getDriverMinorVersion() {
        return AfmDriver.MINOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSQLKeywords() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSQLKeywords is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNumericFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getNumericFunctions is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStringFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getStringFunctions is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSystemFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSystemFunctions is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getTimeDateFunctions is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getExtraNameCharacters() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getExtraNameCharacters is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsColumnAliasing() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsConvert() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsExpressionsInOrderBy() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsGroupBy() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCatalogAtStart() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCatalogSeparator() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getCatalogSeparator is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsUnion() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxBinaryLiteralLength() {
        return 1024;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxCharLiteralLength() {
        return 1024;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxColumnNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxColumnsInGroupBy() {
        return 16;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxColumnsInIndex() {
        return 16;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxColumnsInOrderBy() {
        return 16;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxColumnsInSelect() {
        return 32;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxColumnsInTable() {
        return 256;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxConnections() {
        return 16;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxCursorNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxIndexLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxSchemaNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxProcedureNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxCatalogNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxRowSize() {
        return 16384;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxStatementLength() {
        return 16384;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxStatements() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxTableNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxTablesInSelect() {
        return 16;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxUserNameLength() {
        return 64;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getDefaultTransactionIsolation is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsTransactions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern) {
        return AfmDatabaseMetadataResultSets.emptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getProcedureColumns is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) {
        // LOGGER.info(String.format("getTables catalog='%s' schemaPattern='%s' tableNamePattern='%s' types='%s'", catalog, schemaPattern, tableNamePattern, types));
        return AfmDatabaseMetadataResultSets.tableResultSet(this.getWorkspaceForSchema(this.schema));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getSchemas() {
        //LOGGER.info("getSchemas");
        // List the current schema only
        return AfmDatabaseMetadataResultSets.schemaResultSet(this.schemas, this.schema.getSchemaName());
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getCatalogs() {
        return AfmDatabaseMetadataResultSets.catalogResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTableTypes() {
        return AfmDatabaseMetadataResultSets.tableTypeResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) {
        //LOGGER.info(String.format("getColumns catalog='%s' schemaPattern='%s' tableNamePattern='%s' columnNamePattern='%s'", catalog, schemaPattern, tableNamePattern, columnNamePattern));
        return AfmDatabaseMetadataResultSets.columnResultSet(this.catalog, this.getWorkspaceForSchema(this.schema));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getColumnPrivileges is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getTablePrivileges is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table,
                                          int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getBestRowIdentifier is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getVersionColumns(String catalog, String schema,
                                       String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getVersionColumns is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
        return AfmDatabaseMetadataResultSets.emptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
        return AfmDatabaseMetadataResultSets.emptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getExportedKeys is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                       String parentTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getCrossReference is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTypeInfo() {
        // TBD need to return
        return AfmDatabaseMetadataResultSets.emptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getIndexInfo(String catalog, String schema,
                                  String table, boolean unique,
                                  boolean approximate) {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
        return AfmDatabaseMetadataResultSets.emptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsResultSetType(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern, int[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getUDTs is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() {
        return this.afmConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                   String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSuperTypes is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern,
                                    String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getSuperTables is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern,
                                   String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getAttributes is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getDatabaseMajorVersion() {
        return AfmDriver.MAJOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getDatabaseMinorVersion() {
        return AfmDriver.MINOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getJDBCMajorVersion() {
        return AfmDriver.MAJOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getJDBCMinorVersion() {
        return AfmDriver.MINOR_VERSION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getRowIdLifetime is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        //LOGGER.info(String.format("getSchemas catalog='%s' schemaPattern='%s'",catalog, schemaPattern));
        // List the current schema only
        return AfmDatabaseMetadataResultSets.schemaResultSet(this.schemas, this.schema.getSchemaName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getClientInfoProperties is not implemented yet");
        //return this.emptyResultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        //throw new SQLFeatureNotSupportedException("Not supported yet");
        return AfmDatabaseMetadataResultSets.emptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getFunctionColumns is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                      String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.getPseudoColumns is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.unwrap is not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData.isWrapperFor is not implemented yet");
    }
}
