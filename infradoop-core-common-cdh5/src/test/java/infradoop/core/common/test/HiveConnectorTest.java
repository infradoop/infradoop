package infradoop.core.common.test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;
import infradoop.core.common.source.ConnectorManager;
import infradoop.core.common.source.HiveConnector;

public class HiveConnectorTest {
	@Test
	public void test() throws SQLException, IOException {
		Account account = AccountManager.register("default");
		try (HiveConnector hive = ConnectorManager.get(account, HiveConnector.class)) {
			try (Statement stmt = hive.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("show databases")) {
					while (rs.next()) {
						System.err.println(rs.getString(1));
					}
				}
			}
		}
	}
}
