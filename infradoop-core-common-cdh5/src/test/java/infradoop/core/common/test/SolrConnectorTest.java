package infradoop.core.common.test;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.source.ConnectorManager;
import infradoop.core.common.source.SolrConnector;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SolrConnectorTest {
	private static EntityDescriptor entityDynamicDescriptor;
	private static EntityDescriptor entityDescriptor;
	
	@BeforeClass
	public static void register_account() throws IOException {
		AccountManager.register(Account.DEFAULT);
	}
	
	@BeforeClass
	public static void create_entities_descriptor() {
		entityDynamicDescriptor = new EntityDescriptor("default", "test_dynamic")
				.compileAttribute("id string set \"${name}_${order}\" required")
				.compileAttribute("name string required")
				.compileAttribute("order int required");
		entityDynamicDescriptor.setDynamicFields(true);
		entityDynamicDescriptor.setShards(2);
		
		entityDescriptor = new EntityDescriptor("default", "test")
				.compileAttribute("id string set \"${name}_${order}\" required")
				.compileAttribute("name string required")
				.compileAttribute("order int required");
		entityDescriptor.setDynamicFields(false);
		entityDescriptor.setShards(2);
	}
	
	@Test
	public void test_01_list_entities() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			for (String domain : solr.getDomains()) {
				for (String entity : solr.getEntities(domain)) {
					System.out.println(domain+"."+entity);
				}
			}
		}
	}
	
	@Test
	public void test_02_create_entity_dynamic() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			if (solr.entityExists(entityDynamicDescriptor))
				solr.dropEntity(entityDynamicDescriptor);
			solr.createEntity(entityDynamicDescriptor);
		}
	}
	
	@Test
	public void test_03_fill_entity_dynamic() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			try (EntityWriter writer = solr.getEntityWriter(entityDynamicDescriptor)) {
				writer.setValue(1, "aaa");
				writer.setValue(2, 1);
				writer.write();
				writer.setValue(1, "bbb");
				writer.setValue(2, 2);
				writer.write();
				writer.setValue(1, "ccc");
				writer.setValue(2, 3);
				writer.write();
			}
		}
	}
	
	@Test
	public void test_04_build_entity_dynamic_descriptor() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			EntityDescriptor edd = solr.buildEntityDescriptor(entityDynamicDescriptor);
			Assert.assertTrue(edd.useDynamicFields());
			int fields = 0;
			for (String an : edd.getAttributesNames()) {
				System.out.println(edd.getAttribute(an).toString());
				fields++;
			}
			Assert.assertEquals(fields, 1);
		}
	}
	
	@Test
	public void test_05_create_entity() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			if (solr.entityExists(entityDescriptor))
				solr.dropEntity(entityDescriptor);
			solr.createEntity(entityDescriptor);
		}
	}
	
	@Test
	public void test_06_fill_entity() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			try (EntityWriter writer = solr.getEntityWriter(entityDescriptor)) {
				writer.setValue(1, "aaa");
				writer.setValue(2, 1);
				writer.write();
				writer.setValue(1, "bbb");
				writer.setValue(2, 2);
				writer.write();
				writer.setValue(1, "ccc");
				writer.setValue(2, 3);
				writer.write();
			}
		}
	}
	
	@Test
	public void test_07_build_entity_descriptor() throws IOException {
		try (SolrConnector solr = ConnectorManager.get(
				AccountManager.get(Account.DEFAULT), SolrConnector.class)) {
			EntityDescriptor edd = solr.buildEntityDescriptor(entityDescriptor);
			Assert.assertFalse(edd.useDynamicFields());
			int fields = 0;
			for (String an : edd.getAttributesNames()) {
				System.out.println(edd.getAttribute(an).toString());
				fields++;
			}
			Assert.assertEquals(fields, 3);
		}
	}
}
