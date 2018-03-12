package infradoop.core.common.test;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;
import infradoop.core.common.source.ConnectorManager;
import infradoop.core.common.source.HttpClientConnector;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Assume;
import org.junit.Test;

public class HttpClientConnectorTest {
	@Test
	public void test() throws IOException {
		Assume.assumeNotNull(System.getProperty("account.1.principal"));
		Assume.assumeNotNull(System.getProperty("account.1.password"));
		Assume.assumeNotNull(System.getProperty("restful.url"));
		
		Account account = AccountManager.register("account.1",
				System.getProperty("account.1.principal"),
				System.getProperty("account.1.password"));
		
		try (HttpClientConnector http = ConnectorManager.get(account, HttpClientConnector.class)) {
			HttpGet request = new HttpGet(System.getProperty("restful.url"));
			HttpResponse resp = http.execute(request);
			try (BufferedReader reader = new BufferedReader(
					new InputStreamReader(resp.getEntity().getContent()))) {
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
				}
			}
		}
	}
}
