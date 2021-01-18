package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.metadata.AfmDatabaseMetaData;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.GoodDataEndpoint;
import com.gooddata.sdk.service.GoodDataSettings;
import com.gooddata.sdk.service.httpcomponents.LoginPasswordGoodDataRestProvider;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC driver connection
 */
public class AfmConnection implements java.sql.Connection {

    private final static Logger LOGGER = Logger.getLogger(AfmConnection.class.getName());

    private final GoodData gd;
    private final AfmDatabaseMetaData afmDatabaseMetaData;

    private boolean isClosed = false;
    private boolean autoCommit = false;
    private Properties clientInfo = new Properties();
    private final Project workspace;

    private String schema = null;

    /**
     * Constructor
     *
     * @param url        JDBC URL
     * @param properties connection properties
     * @throws SQLException SQL problem
     * @throws IOException  other problem
     */
    public AfmConnection(final String url,
                         final Properties properties) throws SQLException, IOException {

        String login = properties.getProperty("user");
        String password = properties.getProperty("password");
        Pattern p = Pattern.compile("^jdbc:gd://(.*?)/gdc/projects/(.*?)$");
        Matcher m = p.matcher(url);
        boolean matches = m.matches();
        if (matches && m.groupCount() != 2)
            throw new SQLException(String.format("Wrong JDBC URL format: '%s'", url));
        String pid = m.group(2);
        String host = m.group(1);
        this.gd = new GoodData(host, login, password);
        this.workspace = gd.getProjectService().getProjectById(pid);

        LoginPasswordGoodDataRestProvider lp = new LoginPasswordGoodDataRestProvider(
                new GoodDataEndpoint(host, GoodDataEndpoint.PORT, GoodDataEndpoint.PROTOCOL),
                new GoodDataSettings(),
                login,
                password);
        RestTemplate gdRestTemplate = lp.getRestTemplate();
        this.afmDatabaseMetaData = new AfmDatabaseMetaData(
                this, this.gd, workspace, login, gdRestTemplate);

    }

    /**
     * Refreshes the AFM catalog
     * @throws SQLException connectivity issues
     */
    public void refreshCatalog() throws SQLException {
        this.afmDatabaseMetaData.getCatalog().populate(this.gd, this.workspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Statement createStatement() {
        AfmStatement s =  new AfmStatement(this, this.gd, this.afmDatabaseMetaData);
        /*
        return (AfmStatement) Proxy.newProxyInstance(
                s.getClass().getClassLoader(),
                new Class[] { s.getClass() },
                new LoggingInvocationHandler(s));
         */
        return s;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.prepareStatement is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.prepareCall is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.nativeSQL is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getAutoCommit() {
        return this.autoCommit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.gd.logout();
        this.isClosed = true;
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
    public java.sql.DatabaseMetaData getMetaData() {
        return this.afmDatabaseMetaData;
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
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setReadOnly is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCatalog() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(String catalog) {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getTransactionIsolation is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setTransactionIsolation is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getWarnings is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getWarnings is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) {
        return this.createStatement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.prepareCall(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getTypeMap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setTypeMap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getHoldability is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setHoldability is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setSavepoint is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setSavepoint is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.rollback is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.releaseSavepoint is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return this.createStatement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return this.prepareCall(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createClob is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createBlob is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createNClob is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createSQLXML is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(int timeout) {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientInfo(String name, String value) {
        this.clientInfo.put(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientInfo(String name) {
        return (String) this.clientInfo.get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Properties getClientInfo() {
        return this.clientInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientInfo(Properties properties) {
        this.clientInfo = properties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createArrayOf is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createStruct is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSchema() {
        return this.afmDatabaseMetaData.getWorkspaceId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort(Executor executor) {
        this.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setNetworkTimeout is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getNetworkTimeout is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.unwrap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.isWrapperFor is not supported yet.");
    }


}
