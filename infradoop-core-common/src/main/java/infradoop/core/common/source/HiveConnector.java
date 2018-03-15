package infradoop.core.common.source;

import java.sql.Connection;
import java.sql.SQLException;

import infradoop.core.common.account.Account;

public abstract class HiveConnector extends AbstractConnectorEntityHandler implements Connection {
	public HiveConnector(Account account, Object connection) {
		super(account, connection);
	}
	
	@Override
	public String getConnectorType() {
		return "hive";
	}
	
	@Override
	public void close() throws SQLException {
		try {
			super.close();
		} catch (Exception e) {
			throw new SQLException("unable to close connector "
					+ "["+getConnectorType()+", "+getAccount().getName()+"]", e);
		}
	}
}
