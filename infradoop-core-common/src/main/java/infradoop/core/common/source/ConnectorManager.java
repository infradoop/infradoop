package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

public class ConnectorManager {
	private static final Logger LOG = Logger.getLogger(ConnectorManager.class);
	
	private static List<ConnectorFactory> factories;
	private static GenericKeyedObjectPool<String, Connector> pool;
	private static Map<String, PoolKey> poolKeys;
	
	public static void closePool() {
		try {
			getPool().close();
		} catch (Exception e) {
			LOG.error("unable to close connector pool", e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <C extends Connector> C get(Account account, Class<C> classType) throws IOException {
		if (account == null)
			throw new IllegalArgumentException("unable to get connector with null account ["+classType.getName()+"]");
		try {
			return (C)getPool().borrowObject(getPoolKey(account, getFactory(classType)));
		} catch (Exception e) {
			throw new IOException("unable to get connector "
					+ "["+account.getName()+", "+classType.getName()+"]", e);
		}
	}
	@SuppressWarnings("unchecked")
	public static <C extends Connector> C create(Account account, Class<C> classType) throws IOException {
		if (account == null)
			throw new IllegalArgumentException("unable to get connector with null account ["+classType.getName()+"]");
		try {
			return (C)getFactory(classType).create(account);
		} catch (Exception e) {
			throw new IOException("unable to create connector "
					+ "["+account.getName()+", "+classType.getName()+"]", e);
		}
	}
	
	private static GenericKeyedObjectPool<String, Connector> getPool() {
		if (pool == null) {
			PoolKeyFactory factory;
			pool = new GenericKeyedObjectPool<>(factory = new PoolKeyFactory());
			pool.setMinEvictableIdleTimeMillis(1000L*60L*15L);
			pool.setTimeBetweenEvictionRunsMillis(1000L*60L);
			pool.setMaxActive(8);
			pool.setMinIdle(0);
			factory.setPool(pool);
			LOG.debug("connector pool created");
		}
		return pool;
	}
	private static String getPoolKey(Account account, ConnectorFactory factory) {
		String poolKeyName = account.getName()+"."+factory.getClass().getName();
		if (getPoolKeys().get(poolKeyName) == null)
			getPoolKeys().put(poolKeyName, new PoolKey(account, factory));
		return poolKeyName;
	}
	private static PoolKey getPoolKey(String key) {
		return getPoolKeys().get(key);
	}
	private static Map<String, PoolKey> getPoolKeys() {
		if (poolKeys == null)
			poolKeys = new HashMap<>();
		return poolKeys;
	}
	
	private static ConnectorFactory getFactory(Class<?> classType) {
		for (ConnectorFactory cf : getFactories()) {
			if (cf.accept(classType))
				return cf;
		}
		throw new IllegalArgumentException("unable to find connector factory ["+classType+"]");
	}
	private static List<ConnectorFactory> getFactories() {
		if (factories == null) {
			factories = new ArrayList<>();
			for (ConnectorFactory cf : ServiceLoader.load(ConnectorFactory.class)) {
				factories.add(cf);
				LOG.debug("connector factory registered ["+cf.getClass()+"]");
			}
		}
		return factories;
	}
	
	private static class PoolKey {
		private final Account account;
		private final ConnectorFactory factory;
		public PoolKey(Account account, ConnectorFactory factory) {
			this.account = account;
			this.factory = factory;
		}
		public Account getAccount() {
			return account;
		}
		public ConnectorFactory getFactory() {
			return factory;
		}
	}
	private static class PoolKeyFactory extends BaseKeyedPoolableObjectFactory<String, Connector> {
		private KeyedObjectPool<String, Connector> pool;
		public void setPool(KeyedObjectPool<String, Connector> pool) {
			this.pool = pool;
		}
		@Override
		public Connector makeObject(String key) throws Exception {
			String poolKeyName = (String)key;
			PoolKey poolKey = ConnectorManager.getPoolKey(poolKeyName);
			Connector connector = poolKey.getFactory().create(poolKey.getAccount());
			connector.setPool(poolKeyName, pool);
			return connector;
		}
		@Override
		public void destroyObject(String key, Connector obj) throws Exception {
			try (Connector connector = (Connector)obj) {
				connector.setPool(null, null);
			}
		}
	}
}
