package infradoop.core.common.source;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.hive.jdbc.HiveConnection;

import infradoop.core.common.account.Account;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityNameable;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;
import infradoop.core.common.entity.Nameable;

public class Cdh5HiveConnector extends HiveConnector {
	public Cdh5HiveConnector(Account account, HiveConnection connection) {
		super(account, connection);
	}
	
	@Override
	public String[] getDomains() throws IOException {
		try (Statement stmt = createStatement()) {
			List<String> domains = new ArrayList<>();
			try (ResultSet rs = stmt.executeQuery("show databases")) {
				while (rs.next())
					domains.add(rs.getString(1));
			}
			return domains.toArray(new String[domains.size()]);
		} catch (SQLException e) {
			throw new IOException("unable to get domains "
					+ "["+getConnectorType()+", "+getAccount().getName()+"]", e);
		}
	}
	@Override
	public String[] getEntities(String domain) throws IOException {
		try (Statement stmt = createStatement()) {
			List<String> entities = new ArrayList<>();
			try (ResultSet rs = stmt.executeQuery("show tables in "+domain)) {
				while (rs.next())
					entities.add(rs.getString(1));
			}
			return entities.toArray(new String[entities.size()]);
		} catch (SQLException e) {
			throw new IOException("unable to get domains "
					+ "["+getConnectorType()+", "+getAccount().getName()+"]", e);
		}
	}
	
	@Override
	public EntityDescriptor buildEntityDescriptor(String entity) throws IOException {
		return buildEntityDescriptor(new EntityNameable(entity));
	}
	@Override
	public EntityDescriptor buildEntityDescriptor(String domain, String entity) throws IOException {
		return buildEntityDescriptor(new EntityNameable(domain, entity));
	}
	@Override
	public EntityDescriptor buildEntityDescriptor(Nameable nameable) throws IOException {
		// TODO Pendiente de implementar
		return null;
	}
	
	@Override
	public void createEntity(EntityDescriptor entity) throws IOException {
		// TODO Pendiente de implementar
	}
	@Override
	public void dropEntity(Nameable nameable) throws IOException {
		// TODO Pendiente de implementar
	}
	@Override
	public boolean entityExists(Nameable nameable) throws IOException {
		// TODO Pendiente de implementar
		return false;
	}
	@Override
	public EntityWriter getEntityWriter(EntityDescriptor entityDesc, EntityWriterOptions options) throws IOException {
		// TODO Pendiente de implementar
		return null;
	}
	
	@Override
	public Statement createStatement() throws SQLException {
		return ((HiveConnection)connection).createStatement();
	}
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return ((HiveConnection)connection).prepareStatement(sql);
	}
	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return ((HiveConnection)connection).prepareCall(sql);
	}
	@Override
	public String nativeSQL(String sql) throws SQLException {
		return ((HiveConnection)connection).nativeSQL(sql);
	}
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		((HiveConnection)connection).setAutoCommit(autoCommit);
	}
	@Override
	public boolean getAutoCommit() throws SQLException {
		return ((HiveConnection)connection).getAutoCommit();
	}
	@Override
	public void commit() throws SQLException {
		((HiveConnection)connection).commit();
	}
	@Override
	public void rollback() throws SQLException {
		((HiveConnection)connection).rollback();
	}
	@Override
	public boolean isClosed() throws SQLException {
		return ((HiveConnection)connection).isClosed();
	}
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return ((HiveConnection)connection).getMetaData();
	}
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		((HiveConnection)connection).setReadOnly(readOnly);
	}
	@Override
	public boolean isReadOnly() throws SQLException {
		return ((HiveConnection)connection).isReadOnly();
	}
	@Override
	public void setCatalog(String catalog) throws SQLException {
		((HiveConnection)connection).setCatalog(catalog);
	}
	@Override
	public String getCatalog() throws SQLException {
		return ((HiveConnection)connection).getCatalog();
	}
	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		((HiveConnection)connection).setTransactionIsolation(level);
	}
	@Override
	public int getTransactionIsolation() throws SQLException {
		return ((HiveConnection)connection).getTransactionIsolation();
	}
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return ((HiveConnection)connection).getWarnings();
	}
	@Override
	public void clearWarnings() throws SQLException {
		((HiveConnection)connection).clearWarnings();
	}
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return ((HiveConnection)connection).createStatement(resultSetType, resultSetConcurrency);
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return ((HiveConnection)connection).prepareStatement(sql, resultSetType, resultSetConcurrency);
	}
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return ((HiveConnection)connection).prepareCall(sql, resultSetType, resultSetConcurrency);
	}
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return ((HiveConnection)connection).getTypeMap();
	}
	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		((HiveConnection)connection).setTypeMap(map);
	}
	@Override
	public void setHoldability(int holdability) throws SQLException {
		((HiveConnection)connection).setHoldability(holdability);
	}
	@Override
	public int getHoldability() throws SQLException {
		return ((HiveConnection)connection).getHoldability();
	}
	@Override
	public Savepoint setSavepoint() throws SQLException {
		return ((HiveConnection)connection).setSavepoint();
	}
	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return ((HiveConnection)connection).setSavepoint(name);
	}
	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		((HiveConnection)connection).rollback(savepoint);
	}
	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		((HiveConnection)connection).releaseSavepoint(savepoint);
	}
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return ((HiveConnection)connection).createStatement(
				resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return ((HiveConnection)connection).prepareStatement(
				sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return ((HiveConnection)connection).prepareCall(
				sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return ((HiveConnection)connection).prepareStatement(sql, autoGeneratedKeys);
	}
	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return ((HiveConnection)connection).prepareStatement(sql, columnIndexes);
	}
	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return ((HiveConnection)connection).prepareStatement(sql, columnNames);
	}
	@Override
	public Clob createClob() throws SQLException {
		return ((HiveConnection)connection).createClob();
	}
	@Override
	public Blob createBlob() throws SQLException {
		return ((HiveConnection)connection).createBlob();
	}
	@Override
	public NClob createNClob() throws SQLException {
		return ((HiveConnection)connection).createNClob();
	}
	@Override
	public SQLXML createSQLXML() throws SQLException {
		return ((HiveConnection)connection).createSQLXML();
	}
	@Override
	public boolean isValid(int timeout) throws SQLException {
		return ((HiveConnection)connection).isValid(timeout);
	}
	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		((HiveConnection)connection).setClientInfo(name, value);
	}
	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		((HiveConnection)connection).setClientInfo(properties);
	}
	@Override
	public String getClientInfo(String name) throws SQLException {
		return ((HiveConnection)connection).getClientInfo(name);
	}
	@Override
	public Properties getClientInfo() throws SQLException {
		return ((HiveConnection)connection).getClientInfo();
	}
	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return ((HiveConnection)connection).createArrayOf(typeName, elements);
	}
	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return ((HiveConnection)connection).createStruct(typeName, attributes);
	}
	@Override
	public void setSchema(String schema) throws SQLException {
		((HiveConnection)connection).setSchema(schema);
	}
	@Override
	public String getSchema() throws SQLException {
		return ((HiveConnection)connection).getSchema();
	}
	@Override
	public void abort(Executor executor) throws SQLException {
		((HiveConnection)connection).abort(executor);
	}
	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		((HiveConnection)connection).setNetworkTimeout(executor, milliseconds);
	}
	@Override
	public int getNetworkTimeout() throws SQLException {
		return ((HiveConnection)connection).getNetworkTimeout();
	}
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return ((HiveConnection)connection).unwrap(iface);
	}
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return ((HiveConnection)connection).isWrapperFor(iface);
	}
}
