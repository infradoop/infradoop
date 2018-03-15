package infradoop.core.common.source;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.util.NamedList;

import infradoop.core.common.account.Account;

public class Cdh5SimpleHttpSolrServer extends HttpSolrServer {
	private final Account account;

	public Cdh5SimpleHttpSolrServer(Account account, String host) {
		super(host);
		this.account = account;
	}

	@Override
	public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
		try {
			return account.getUserGroupInformation().doAs(
				new PrivilegedExceptionAction<NamedList<Object>>() {
					@Override
					public NamedList<Object> run() throws Exception {
						return Cdh5SimpleHttpSolrServer.super.request(request);
					}
				});
		} catch (InterruptedException e) {
			throw new SolrServerException("unable to execute solr request with kerberos", e);
		}
	}
}
