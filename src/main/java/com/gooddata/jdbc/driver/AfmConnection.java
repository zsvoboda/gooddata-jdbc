package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.metadata.AfmDatabaseMetaData;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.GoodDataEndpoint;
import com.gooddata.sdk.service.GoodDataSettings;
import com.gooddata.sdk.service.httpcomponents.LoginPasswordGoodDataRestProvider;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
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
    private String catalog = "";

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
        LOGGER.info(String.format("AfmConnection: url='%s', properties='%s'", url, properties));
        String login = properties.getProperty("user");
        String password = properties.getProperty("password");
        Pattern p = Pattern.compile("^jdbc:gd://(.*?)/gdc/projects/(.*?)$");
        Matcher m = p.matcher(url);
        boolean matches = m.matches();
        if (matches && m.groupCount() != 2)
            throw new SQLException(String.format("Wrong JDBC URL format: '%s'", url));
        String host = m.group(1);
        String pid = m.group(2);
        this.gd = new GoodData(host, login, password);
        LoginPasswordGoodDataRestProvider lp = new LoginPasswordGoodDataRestProvider(
                new GoodDataEndpoint(host, GoodDataEndpoint.PORT, GoodDataEndpoint.PROTOCOL),
                new GoodDataSettings(),
                login,
                password);
        RestTemplate gdRestTemplate = lp.getRestTemplate();
        this.afmDatabaseMetaData = new AfmDatabaseMetaData(
                this, this.gd, pid, login, gdRestTemplate);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Statement createStatement() {
        LOGGER.info("createStatement");
        return new AfmStatement(this, this.gd, this.afmDatabaseMetaData, "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql) {
        LOGGER.info(String.format("prepareStatement sql='%s'", sql));
        return new AfmStatement(this, this.gd, this.afmDatabaseMetaData, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        LOGGER.info(String.format("prepareCall sql='%s'", sql));
        throw new SQLFeatureNotSupportedException("Connection.prepareCall is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String nativeSQL(String sql) {
        LOGGER.info(String.format("nativeSQL sql='%s'", sql));
        return sql;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getAutoCommit() {
        LOGGER.info("getAutoCommit");
        return this.autoCommit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAutoCommit(boolean autoCommit) {
        LOGGER.info(String.format("setAutoCommit autoCommit='%s'", autoCommit));
        this.autoCommit = autoCommit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        LOGGER.info("commit");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() {
        LOGGER.info("rollback");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOGGER.info("close");
        this.gd.logout();
        this.isClosed = true;
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
    public java.sql.DatabaseMetaData getMetaData() {
        return this.afmDatabaseMetaData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() {
        LOGGER.info("isReadOnly");
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        LOGGER.info(String.format("setReadOnly readOnly='%s'", readOnly));
        throw new SQLFeatureNotSupportedException("Connection.setReadOnly is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCatalog() {
        LOGGER.info("getCatalog");
        return this.catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(String catalog) {
        LOGGER.info(String.format("setCatalog catalog='%s'", catalog));
        this.catalog = catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTransactionIsolation() {
        LOGGER.info("getTransactionIsolation");
        return Connection.TRANSACTION_NONE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        LOGGER.info(String.format("setTransactionIsolation level='%d'", level));
        throw new SQLFeatureNotSupportedException("Connection.setTransactionIsolation is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() {
        LOGGER.info("getWarnings");
        return new SQLWarning();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() {
        LOGGER.info("clearWarnings");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) {
        LOGGER.info(String.format("createStatement resultSetType='%d', resultSetConcurrency='%d'",
                resultSetType, resultSetConcurrency));
        return this.createStatement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
        LOGGER.info(String.format("prepareStatement sql='%s', resultSetType='%d', resultSetConcurrency='%d'",
                sql, resultSetType, resultSetConcurrency));
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        LOGGER.info(String.format("prepareCall sql='%s', resultSetType='%d', resultSetConcurrency='%d'",
                sql, resultSetType, resultSetConcurrency));
        return this.prepareCall(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        LOGGER.info("getTypeMap");
        throw new SQLFeatureNotSupportedException("Connection.getTypeMap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        LOGGER.info(String.format("setTypeMap map='%s'", map));
        throw new SQLFeatureNotSupportedException("Connection.setTypeMap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHoldability() throws SQLException {
        LOGGER.info("getHoldability");
        throw new SQLFeatureNotSupportedException("Connection.getHoldability is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        LOGGER.info(String.format("setHoldability holdability='%d'", holdability));
        throw new SQLFeatureNotSupportedException("Connection.setHoldability is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        LOGGER.info("setSavepoint");
        throw new SQLFeatureNotSupportedException("Connection.setSavepoint is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        LOGGER.info(String.format("setSavepoint name='%s'", name));
        throw new SQLFeatureNotSupportedException("Connection.setSavepoint is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        LOGGER.info(String.format("rollback savepoint='%s'", savepoint));
        throw new SQLFeatureNotSupportedException("Connection.rollback is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        LOGGER.info(String.format("releaseSavepoint savepoint='%s'", savepoint));
        throw new SQLFeatureNotSupportedException("Connection.releaseSavepoint is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        LOGGER.info(String.format("createStatement resultsetType='%d', resultSetConcurrency='%d', " +
                "resultSetHoldability='%s'", resultSetType, resultSetConcurrency, resultSetHoldability));
        return this.createStatement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) {
        LOGGER.info(String.format("prepareStatement sql='%s', resultsetType='%d', resultSetConcurrency='%d', " +
                "resultSetHoldability='%s'", sql, resultSetType, resultSetConcurrency, resultSetHoldability));
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        LOGGER.info(String.format("prepareCall sql='%s', resultsetType='%d', resultSetConcurrency='%d', " +
                "resultSetHoldability='%s'", sql, resultSetType, resultSetConcurrency, resultSetHoldability));
        return this.prepareCall(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
        LOGGER.info(String.format("prepareStatement sql='%s', autoGeneratedKeys='%d'", sql, autoGeneratedKeys));
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
        LOGGER.info(String.format("prepareStatement sql='%s', columnIndexes='%s'", sql, Arrays.toString(columnIndexes)));
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {
        LOGGER.info(String.format("prepareStatement sql='%s', columnNames='%s'", sql, Arrays.toString(columnNames)));
        return this.prepareStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Clob createClob() throws SQLException {
        LOGGER.info("createClob");
        throw new SQLFeatureNotSupportedException("Connection.createClob is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Blob createBlob() throws SQLException {
        LOGGER.info("createBlob");
        throw new SQLFeatureNotSupportedException("Connection.createBlob is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NClob createNClob() throws SQLException {
        LOGGER.info("createNClob");
        throw new SQLFeatureNotSupportedException("Connection.createNClob is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        LOGGER.info("createSQLXML");
        throw new SQLFeatureNotSupportedException("Connection.createSQLXML is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(int timeout) {
        LOGGER.info(String.format("isValid timeout='%d'", timeout));
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientInfo(String name, String value) {
        LOGGER.info(String.format("setClientInfo name='%s', value='%s'", name, value));
        this.clientInfo.put(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientInfo(String name) {
        LOGGER.info(String.format("getClientInfo name='%s'", name));
        return (String) this.clientInfo.get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Properties getClientInfo() {
        LOGGER.info("getClientInfo");
        return this.clientInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientInfo(Properties properties) {
        LOGGER.info(String.format("setClientInfo properties='%s'", properties));
        this.clientInfo = properties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        LOGGER.info(String.format("createArrayOf typeName='%s', elements='%s'", typeName, Arrays.toString(elements)));
        throw new SQLFeatureNotSupportedException("Connection.createArrayOf is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        LOGGER.info(String.format("createStruct typeName='%s', attributes='%s'", typeName, Arrays.toString(attributes)));
        throw new SQLFeatureNotSupportedException("Connection.createStruct is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSchema() {
        LOGGER.info("getSchema");
        return this.afmDatabaseMetaData.getSchema();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        LOGGER.info(String.format("setSchema schema='%s'", schema));
        this.afmDatabaseMetaData.setSchema(schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort(Executor executor) {
        LOGGER.info(String.format("abort executor='%s'", executor));
        this.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        LOGGER.info(String.format("setNetworkTimeout executor='%s', milliseconds='%d'", executor, milliseconds));
        throw new SQLFeatureNotSupportedException("Connection.setNetworkTimeout is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        LOGGER.info("getNetworkTimeout");
        throw new SQLFeatureNotSupportedException("Connection.getNetworkTimeout is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        LOGGER.info(String.format("unwrap iface='%s'", iface));
        throw new SQLFeatureNotSupportedException("Connection.unwrap is not supported yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        LOGGER.info(String.format("isWrapperFor iface='%s'", iface));
        throw new SQLFeatureNotSupportedException("Connection.isWrapperFor is not supported yet.");
    }


}
