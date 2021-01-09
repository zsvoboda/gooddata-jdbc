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

    private final static Logger logger = Logger.getGlobal();

    private final GoodData gd;

    private String login;
    private final Project workspace;

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

        logger.info("jdbc4gd: connection constructor url:" + url);

        this.login = properties.getProperty("user");
        String password = properties.getProperty("password");

        logger.info("jdbc4gd: user:" + login);

        Pattern p = Pattern.compile("^jdbc:gd://(.*?)/gdc/projects/(.*?)$");
        Matcher m = p.matcher(url);
        m.matches();

        if (m.groupCount() != 2) throw new SQLException("Wrong JDBC URL format");

        String pid = m.group(2);
        String host = m.group(1);


        logger.info("jdbc4gd: pid:" + pid);
        logger.info("jdbc4gd: server:" + host);

        logger.info("gd init begin");
        this.gd = new GoodData(host, login, password);

        this.workspace = gd.getProjectService().getProjectById(pid);

        logger.info("gd init end");
    }

    public String getLogin() {
        return this.login;
    }

    public Project getWorkspace() {
        return workspace;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection unwrap");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection iswrapperfor");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public java.sql.Statement createStatement() {
        logger.info("jdbc4gd: connection createstatement");
        return new com.gooddata.jdbc.driver.Statement(gd, this);
    }


    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        logger.info("jdbc4gd: connection nativesql");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean getAutoCommit() {
        logger.info("jdbc4gd: connection method");
        return this.autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        logger.info("jdbc4gd: connection method");
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() {
        logger.info("jdbc4gd: connection method");
    }

    @Override
    public void rollback() {
        logger.info("jdbc4gd: connection method");
    }

    @Override
    public void close() {
        logger.info("jdbc4gd: connection method");
        this.gd.logout();
        this.isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() {
        return new DatabaseMetaData(this);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
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
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
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
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
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
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void abort(Executor executor) {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

}
