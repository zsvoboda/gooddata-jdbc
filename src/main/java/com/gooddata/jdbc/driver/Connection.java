package com.gooddata.jdbc.driver;

import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Connection implements java.sql.Connection {

    private final static Logger LOGGER = Logger.getLogger(Connection.class.getName());

    private final GoodData gd;
    private final DatabaseMetaData databaseMetaData;

    private boolean isClosed = false;
    private boolean autoCommit = false;
    private  Properties clientInfo = new Properties();

    /**
     * Constructor
     * @param url JDBC URL
     * @param properties connection properties
     * @throws SQLException SQL problem
     * @throws IOException other problem
     */
    public Connection(final String url,
                      final Properties properties) throws SQLException, IOException {

        String login = properties.getProperty("user");
        String password = properties.getProperty("password");
        Pattern p = Pattern.compile("^jdbc:gd://(.*?)/gdc/projects/(.*?)$");
        Matcher m = p.matcher(url);
        m.matches();
        if (m.groupCount() != 2)
            throw new SQLException(String.format("Wrong JDBC URL format: '%s'", url));
        String pid = m.group(2);
        String host = m.group(1);
        this.gd = new GoodData(host, login, password);
        Project workspace = gd.getProjectService().getProjectById(pid);
        this.databaseMetaData = new DatabaseMetaData(this, this.gd, workspace, login);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.unwrap is not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.isWrapperFor is not supported yet.");
    }

    @Override
    public java.sql.Statement createStatement() {
        return new Statement(this, this.gd, this.databaseMetaData);
    }


    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.prepareStatement is not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.prepareCall is not supported yet.");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.nativeSQL is not supported yet.");
    }

    @Override
    public boolean getAutoCommit() {
        return this.autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void close() {
        this.gd.logout();
        this.isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() {
        return databaseMetaData;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setReadOnly is not supported yet.");
    }

    @Override
    public String getCatalog() throws SQLException {
        return "";
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        //throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getTransactionIsolation is not supported yet.");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setTransactionIsolation is not supported yet.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getWarnings is not supported yet.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getWarnings is not supported yet.");
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return this.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.prepareCall(sql);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getTypeMap is not supported yet.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setTypeMap is not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getHoldability is not supported yet.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setHoldability is not supported yet.");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setSavepoint is not supported yet.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setSavepoint is not supported yet.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.rollback is not supported yet.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.releaseSavepoint is not supported yet.");
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return this.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return this.prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return this.prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return this.prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return this.prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createClob is not supported yet.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createBlob is not supported yet.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createNClob is not supported yet.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createSQLXML is not supported yet.");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) {
        this.clientInfo.put(name, value);
    }

    @Override
    public String getClientInfo(String name) {
        return (String)this.clientInfo.get(name);
    }

    @Override
    public Properties getClientInfo() {
        return this.clientInfo;
    }

    @Override
    public void setClientInfo(Properties properties){
        this.clientInfo = properties;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createArrayOf is not supported yet.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.createStruct is not supported yet.");
    }

    @Override
    public String getSchema() throws SQLException {
        return this.databaseMetaData.getWorkspaceId();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setSchema is not supported yet.");
    }

    @Override
    public void abort(Executor executor) {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.setNetworkTimeout is not supported yet.");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection.getNetworkTimeout is not supported yet.");
    }

}
