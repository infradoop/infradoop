package infradoop.core.common.test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;
import infradoop.core.common.source.ConnectorManager;
import infradoop.core.common.source.HiveConnector;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HiveConnectorTest {
	@BeforeClass
	public static void registerAccount() throws IOException {
		AccountManager.register(Account.INHERIT);
	}
	
	@Test
	public void test_01_showdatabase() throws SQLException, IOException {
		Account account = AccountManager.get(Account.INHERIT);
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
	
	@Test
	public void test_02_query() throws SQLException, IOException {
		Assert.assertNotNull(System.getProperty("hive.table"));
		String table = System.getProperty("hive.table");
		Account account = AccountManager.get(Account.INHERIT);
		try (HiveConnector hive = ConnectorManager.get(account, HiveConnector.class)) {
			try (Statement stmt = hive.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("select count(*) from "+table)) {
					while (rs.next()) {
						System.err.println(rs.getString(1));
					}
				}
			}
		}
	}
}
