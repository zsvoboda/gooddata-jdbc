package com.gooddata.jdbc.driver;

import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Connection implements java.sql.Connection {

    private final static Logger logger = Logger.getGlobal();

    private final GoodData gd;

    private final Project workspace;

    private boolean autoCommit = false;
    private final HashMap<String,String> clientInfo = new HashMap<>();

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

        String login = properties.getProperty("user");
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
    }

    @Override
    public boolean isClosed() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection method");
        return null;//new DatabaseMetaData(gd);
    }

    @Override
    public boolean isReadOnly() {
        logger.info("jdbc4gd: connection method");
        return true;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public String getCatalog() throws SQLException {
        logger.info("jdbc4gd: connection getCatalog");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public com.gooddata.jdbc.driver.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        logger.info("jdbc4gd: connection createstatement2");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection getTypeMap");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public com.gooddata.jdbc.driver.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Clob createClob() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection isValid");
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setClientInfo(String name, String value) {
        logger.info("jdbc4gd: connection method");
        this.clientInfo.put(name, value);
    }

    @Override
    public String getClientInfo(String name) {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection getClientInfo");
        return this.clientInfo.get(name);
    }

    @Override
    public Properties getClientInfo() {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection getClientInfo");
        return null;
    }

    @Override
    public void setClientInfo(Properties properties){
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection method");

    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        logger.info("jdbc4gd: connection method");
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getSchema() {
        logger.info("jdbc4gd: connection getSchema");
        return null;
    }

    @Override
    public void setSchema(String schema) {
        logger.info("jdbc4gd: connection method");
    }

    @Override
    public void abort(Executor executor) {
        logger.info("jdbc4gd: connection method");
        close();

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection method");

    }

    @Override
    public int getNetworkTimeout() {
        // TODO Auto-generated method stub
        logger.info("jdbc4gd: connection method");
        return 0;
    }

}
