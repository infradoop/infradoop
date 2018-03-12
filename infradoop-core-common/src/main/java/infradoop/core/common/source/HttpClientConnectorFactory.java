package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import java.io.IOException;
import java.security.Principal;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.BasicSchemeFactory;
import org.apache.http.impl.auth.KerberosSchemeFactory;
import org.apache.http.impl.auth.NTLMSchemeFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;

public class HttpClientConnectorFactory implements ConnectorFactory {

	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(HttpClientConnector.class);
	}

	@Override
	public Connector create(Account account) throws Exception {
		HttpClientBuilder builder = HttpClientBuilder.create();
		if (account.getUserGroupInformation().hasKerberosCredentials()) {
			builder.setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeProvider>create()
					.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true))
					.build());
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			Credentials use_jaas_creds = new Credentials() {
				@Override
				public String getPassword() { return null; }
				@Override
				public Principal getUserPrincipal() { return null; }
			};
			credentialsProvider.setCredentials(AuthScope.ANY, use_jaas_creds);
			builder.setDefaultCredentialsProvider(credentialsProvider);
			builder.addInterceptorFirst(
			new HttpRequestInterceptor() {
				@Override
				public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
					if (request instanceof HttpEntityEnclosingRequest) {
						HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);
						HttpEntity requestEntity = enclosingRequest.getEntity();
						enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
					}
				}
			});
		} else {
			builder.setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeProvider>create()
					.register(AuthSchemes.BASIC, new BasicSchemeFactory())
					.build());
			if (account.getProperties().getProperty("user") != null) {
				CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
				UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
						account.getProperties().getProperty("user"),
						account.getProperties().getProperty("password"));
				credentialsProvider.setCredentials(AuthScope.ANY, credentials);
				builder.setDefaultCredentialsProvider(credentialsProvider);
			}
		}
		return new HttpClientConnector(account, builder.build());
	}
}
