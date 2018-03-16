package infradoop.core.common.test;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Assume;
import org.junit.Test;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;
import infradoop.core.common.entity.Grant;
import infradoop.core.common.source.ConnectorManager;
import infradoop.core.common.source.HiveConnector;

public class HiveSecureProviderTest {
	private static final Logger LOG = Logger.getLogger(HiveSecureProviderTest.class);
	
	@Test
	public void test_01_hive_get_roles() throws IOException, SQLException {
		Assume.assumeNotNull(System.getProperty("account.admin.principal"));
		Assume.assumeNotNull(System.getProperty("account.admin.password"));
		
		Account account = AccountManager.register(Account.DEFAULT,
				System.getProperty("account.admin.principal"),
				System.getProperty("account.admin.password"));
		try (HiveConnector hive = ConnectorManager.get(account, HiveConnector.class)) {
			for (String database : hive.getDomains()) {
				for (String table : hive.getEntities(database)) {
					LOG.info(database+"."+table);
					for (Grant grant : hive.retriveEntityGrants(database, table)) {
						LOG.info("  "+grant.getGrantee()+", "+grant.getGranteeType().name()
								+", ["+StringUtils.join(grant.getPermissions(), ":")+"]");
					}
				}
			}
		}
	}
}
