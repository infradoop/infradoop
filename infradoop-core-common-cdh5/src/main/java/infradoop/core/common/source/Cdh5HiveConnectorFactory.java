package infradoop.core.common.source;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbc.HiveConnection;

import infradoop.core.common.SystemConfiguration;
import infradoop.core.common.account.Account;

public class Cdh5HiveConnectorFactory implements ConnectorFactory {
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(Cdh5HiveConnector.class);
	}
	
	protected HiveConnection createConnection(Account account) throws Exception {
		Configuration configuration = account.getConfiguration();
		String jdbcUrl;
		if (configuration.get("hive2.server") != null) {
			String hiveServer = configuration.get("hive2.server");
			if (!hiveServer.contains(":"))
				hiveServer += ":10000";
			jdbcUrl = "jdbc:hive2://"+hiveServer+"/"; 
		} else {
			jdbcUrl = configuration.get("hive2.jdbc.url");
		}
		if (jdbcUrl == null)
			throw new IOException("unable to retrive jdbc url from hive2, "
					+ "parameter hive2.jdbc.url not found");
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		HiveConnection connection;
		if (SystemConfiguration.isSecurityEnabled()) {
			if (!jdbcUrl.contains("principal="))
				jdbcUrl += ";principal=hive/_HOST@"+SystemConfiguration.getSystemRealm();
			final String finalJdbcUrl = jdbcUrl;
			connection = (HiveConnection)account.getUserGroupInformation().doAs(
					new PrivilegedExceptionAction<Connection>() {
						@Override
						public Connection run() throws Exception {
							return DriverManager.getConnection(finalJdbcUrl);
						}
					});
		} else {
			connection = (HiveConnection)DriverManager.getConnection(
					jdbcUrl, account.getName(), "");
		}
		return connection;
	}

	@Override
	public Connector create(Account account) throws Exception {
		return new Cdh5HiveConnector(account, createConnection(account));
	}
	
}
