package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.util.NamedList;

public class Cdh5KerberosHttpSolrServer extends HttpSolrServer {
	private final Account account;

	public Cdh5KerberosHttpSolrServer(Account account, String host) {
		super(host, Cdh5KerberosCloudSolrServer.createKerberosHttpClient());
		this.account = account;
	}

	@Override
	public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
		try {
			return account.getUserGroupInformation().doAs(
				new PrivilegedExceptionAction<NamedList<Object>>() {
					@Override
					public NamedList<Object> run() throws Exception {
						return Cdh5KerberosHttpSolrServer.super.request(request);
					}
				});
		} catch (InterruptedException e) {
			throw new SolrServerException("unable to execute solr request with kerberos", e);
		}
	}
}
