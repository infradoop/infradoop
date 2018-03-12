package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import org.apache.commons.pool.KeyedObjectPool;

public interface Connector extends AutoCloseable {
	public String getConnectorType();
	public Account getAccount();
	public Object unwrap();
	
	public void setPool(String poolKey, KeyedObjectPool<String, Connector> pool);
	public String getPoolKey();
	public KeyedObjectPool<String, Connector> getPool();
}
