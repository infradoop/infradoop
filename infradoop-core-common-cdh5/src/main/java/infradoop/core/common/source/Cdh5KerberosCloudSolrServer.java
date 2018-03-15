package infradoop.core.common.source;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeRegistry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.util.NamedList;

import infradoop.core.common.account.Account;

public class Cdh5KerberosCloudSolrServer extends CloudSolrServer {
	private static final HttpRequestInterceptor BUFFERED_INTERCEPTOR = new HttpRequestInterceptor() {
		@Override
		public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
			if (request instanceof HttpEntityEnclosingRequest) {
				HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);
				HttpEntity requestEntity = enclosingRequest.getEntity();
				enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
			}
		}
	};
	
	public static HttpClient createKerberosHttpClient() {
		DefaultHttpClient httpClient = (DefaultHttpClient)HttpClientUtil.createClient(null);
		AuthSchemeRegistry registry = new AuthSchemeRegistry();
		registry.register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory(true));
		httpClient.setAuthSchemes(registry);
		Credentials use_jaas_creds = new Credentials() {
			@Override
			public String getPassword() { return null; }
			@Override
			public Principal getUserPrincipal() { return null; }
		};
		httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, use_jaas_creds);
		httpClient.addRequestInterceptor(BUFFERED_INTERCEPTOR);
		return httpClient;
	}
	
	private final Account account;

	public Cdh5KerberosCloudSolrServer(Account account, String zkHost) {
		super(zkHost, new LBHttpSolrServer(Cdh5KerberosCloudSolrServer.createKerberosHttpClient()));
		this.account = account;
	}
	
	@Override
	public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
		try {
			return account.getUserGroupInformation().doAs(
				new PrivilegedExceptionAction<NamedList<Object>>() {
					@Override
					public NamedList<Object> run() throws Exception {
						return Cdh5KerberosCloudSolrServer.super.request(request);
					}
				});
		} catch (InterruptedException e) {
			throw new SolrServerException("unable to execute solr request with kerberos", e);
		}
	}
}
