package infradoop.core.common.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import infradoop.core.common.account.Account;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityNameable;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;
import infradoop.core.common.entity.Nameable;

public class Cdh5HbaseConnector extends HbaseConnector {
	public Cdh5HbaseConnector(Account account, Object connection) {
		super(account, connection);
	}

	@Override
	public String[] getDomains() throws IOException {
		Connection hbase = (Connection)unwrap();
		try (Admin admin = hbase.getAdmin()) {
			List<String> databases = new ArrayList<>();
			for (NamespaceDescriptor ns : admin.listNamespaceDescriptors()) {
				databases.add(ns.getName());
			}
			return databases.toArray(new String[databases.size()]);
		}
	}
	@Override
	public String[] getEntities(String domain) throws IOException {
		List<String> tables = new ArrayList<>();
		Connection hbase = (Connection)unwrap();
		try (Admin admin = hbase.getAdmin()) {
			String delim = new String(new char[] {TableName.NAMESPACE_DELIM});
			for (HTableDescriptor tb : admin.listTableDescriptorsByNamespace(domain)) {
				TableName tableName = tb.getTableName();
				String p[];
				if (tableName.getNameAsString().contains(delim))
					p = tableName.getNameAsString().split(delim, 2);
				else
					p = new String[] {NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR,
							tableName.getNameAsString()};
				tables.add(p[1]);
			}
		}
		return tables.toArray(new String[tables.size()]);
	}
	@Override
	public void createEntity(EntityDescriptor entity) throws IOException {
		Connection hbase = (Connection)unwrap();
		try (Admin admin = hbase.getAdmin()) {
			TableName tableName = TableName.valueOf(entity.getDomain(), entity.getName());
			try {
				admin.getNamespaceDescriptor(entity.getDomain());
			} catch (NamespaceNotFoundException e) {
				admin.createNamespace(NamespaceDescriptor.create(entity.getDomain()).build());
			}
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			Set<String> families = new HashSet<>();
			for (int i=0;i<entity.countAttributes();i++)
				families.add(entity.getAttribute(i).getFamily());
			for (String f : families)
				tableDesc.addFamily(new HColumnDescriptor(f));
			admin.createTable(tableDesc);
		}
	}
	
	@Override
	public EntityDescriptor buildEntityDescriptor(String entity) throws IOException {
		return buildEntityDescriptor(new EntityNameable(entity));
	}
	@Override
	public EntityDescriptor buildEntityDescriptor(String domain, String entity) throws IOException {
		return buildEntityDescriptor(new EntityNameable(domain, entity));
	}
	@Override
	public EntityDescriptor buildEntityDescriptor(Nameable nameable) throws IOException {
		// TODO Pendiente de implementar
		return null;
	}
	
	@Override
	public void dropEntity(Nameable nameable) throws IOException {
		Connection hbase = (Connection)unwrap();
		try (Admin admin = hbase.getAdmin()) {
			TableName tableName = TableName.valueOf(nameable.getDomain(), nameable.getName());
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}
	@Override
	public boolean entityExists(Nameable nameable) throws IOException {
		Connection hbase = (Connection)unwrap();
		try (Admin admin = hbase.getAdmin()) {
			return admin.tableExists(TableName.valueOf(
					nameable.getDomain(), nameable.getName()));
		}
	}
	
	@Override
	public EntityWriter getEntityWriter(EntityDescriptor entityDesc, EntityWriterOptions options) throws IOException {
		EntityWriter entityWriter = new Cdh5HbaseEntityWriter(this, entityDesc, options);
		entityWriter.initialize();
		return entityWriter;
	}
}
