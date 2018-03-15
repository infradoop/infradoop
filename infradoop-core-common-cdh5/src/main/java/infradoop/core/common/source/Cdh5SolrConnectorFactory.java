package infradoop.core.common.source;

import java.io.IOException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;

import infradoop.core.common.SystemConfiguration;
import infradoop.core.common.account.Account;

public class Cdh5SolrConnectorFactory implements ConnectorFactory {
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(Cdh5SolrConnector.class);
	}
	
	protected CloudSolrServer createConnection(Account account) throws Exception {
		String solrQuorum = account.getConfiguration().get("solr.zookeeper.quorum",
				account.getConfiguration().get("hbase.zookeeper.quorum"));
		if (solrQuorum == null || "".equals(solrQuorum))
			throw new IOException("unable to retrive solr zookeepr quorum, "
					+ "parameter solr.zookeeper.quorum not found");
		solrQuorum += "/solr";
		CloudSolrServer solrServer;
		if (SystemConfiguration.isSecurityEnabled()) {
			solrServer = new Cdh5KerberosCloudSolrServer(account, solrQuorum);
		} else {
			solrServer = new CloudSolrServer(solrQuorum);
		}
		solrServer.connect();
		return solrServer;
	}
	
	@Override
	public Connector create(Account account) throws Exception {
		return new Cdh5SolrConnector(account, createConnection(account));
	}
}
