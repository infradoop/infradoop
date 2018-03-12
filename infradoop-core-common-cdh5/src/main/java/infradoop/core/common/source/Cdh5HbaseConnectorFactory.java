package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

public class Cdh5HbaseConnectorFactory implements ConnectorFactory {
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(Cdh5HbaseConnector.class);
	}
	
	protected Connection createConnection(Account account) throws Exception {
		return ConnectionFactory.createConnection(account.getConfiguration(),
				User.create(account.getUserGroupInformation()));
	}

	@Override
	public Connector create(Account account) throws Exception {
		return new Cdh5HbaseConnector(account, createConnection(account));
	}

}
