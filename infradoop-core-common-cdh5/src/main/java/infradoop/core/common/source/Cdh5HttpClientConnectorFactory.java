package infradoop.core.common.source;

import java.io.IOException;
import java.security.Principal;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeRegistry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.protocol.HttpContext;

import infradoop.core.common.account.Account;

public class Cdh5HttpClientConnectorFactory implements ConnectorFactory {
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
	public static void configureForKerberos(DefaultHttpClient httpClient) {
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
	}
	private static void configureForBasic(DefaultHttpClient httpClient, String user, String password) {
		AuthSchemeRegistry registry = new AuthSchemeRegistry();
		registry.register(AuthPolicy.BASIC, new SPNegoSchemeFactory(true));
		httpClient.setAuthSchemes(registry);
		if (user != null) {
			Credentials basicCreds = new UsernamePasswordCredentials(user, password);
			httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, basicCreds);
		}
	}
	
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(HttpClientConnector.class);
	}

	@Override
	public Connector create(Account account) throws Exception {
		DefaultHttpClient httpClient = new SystemDefaultHttpClient();
		if (account.getUserGroupInformation().hasKerberosCredentials()) {
			configureForKerberos(httpClient);
		} else {
			configureForBasic(httpClient,
					account.getProperties().getProperty("user"),
					account.getProperties().getProperty("password"));
		}
		return new HttpClientConnector(account, httpClient);
	}
}
