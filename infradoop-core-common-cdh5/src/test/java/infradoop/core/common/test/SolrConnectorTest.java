package infradoop.core.common.test;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.source.ConnectorManager;
import infradoop.core.common.source.SolrConnector;
import java.io.IOException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SolrConnectorTest {
	@Test
	public void test_03_build_entity() throws IOException {
		Account account = AccountManager.register("default");
		try (SolrConnector solr = ConnectorManager.get(account, SolrConnector.class)) {
			EntityDescriptor entity = solr.buildEntityDescriptor("infra_information", "permissions");
			for (String an : entity.getAttributesNames()) {
				System.out.println(entity.getAttribute(an).toString());
			}
		}
	}
}
