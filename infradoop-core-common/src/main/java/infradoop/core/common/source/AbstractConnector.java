package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.log4j.Logger;

public abstract class AbstractConnector implements Connector {
	private static final Logger LOG = Logger.getLogger(HdfsConnector.class);
	
	protected final Account account;
	protected final Object connection;
	
	private String poolKey;
	private KeyedObjectPool<String, Connector> pool;
	
	public AbstractConnector(Account account, Object connection) {
		this.account = account;
		this.connection = connection;
		LOG.debug("connector created ["+getConnectorType()+", "+account.getName()+"]");
	}
	
	@Override
	public Account getAccount() {
		return account;
	}
	@Override
	public Object unwrap() {
		return connection;
	}
	
	@Override
	public void setPool(String poolKey, KeyedObjectPool<String, Connector> pool) {
		this.poolKey = poolKey;
		this.pool = pool;
	}
	@Override
	public String getPoolKey() {
		return poolKey;
	}
	@Override
	public KeyedObjectPool<String, Connector> getPool() {
		return pool;
	}
	
	@Override
	public void close() throws Exception {
		if (getPool() == null) {
			if (connection instanceof AutoCloseable)
				((AutoCloseable)connection).close();
			LOG.debug("connector closed ["+getConnectorType()+", "+account.getName()+"]");
		} else {
			getPool().returnObject(getPoolKey(), this);
		}
	}
}
