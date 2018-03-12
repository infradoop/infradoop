package infradoop.core.common.source;

import infradoop.core.common.account.Account;

public interface ConnectorFactory {
	public boolean accept(Class<?> classType);
	public Connector create(Account account) throws Exception;
}
