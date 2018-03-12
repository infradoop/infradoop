package infradoop.core.common.account;

import infradoop.core.common.KerberosAuthenticator;
import infradoop.core.common.SystemConfiguration;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class DefaultAccount implements Account {
	private final String name;
	private final Properties properties;
	private final KerberosAuthenticator kerberosAuthenticator;
	private final Configuration configuration;
	private final long lifeTimeUserGroupInformation;
	
	private UserGroupInformation userGroupInformation;
	private long lastRenewUserGroupInformation;
	
	public DefaultAccount(String name, Properties properties, KerberosAuthenticator kerberosAuthenticator,
			UserGroupInformation userGroupInformation, Configuration configuration) {
		this.name = name;
		this.properties = properties;
		this.kerberosAuthenticator = kerberosAuthenticator;
		this.userGroupInformation = userGroupInformation;
		this.configuration = configuration;
		
		lastRenewUserGroupInformation = System.currentTimeMillis();
		String kttf = configuration.get("infradoop.kerberos.ticket.timelife",
				System.getProperty("infradoop.kerberos.ticket.timelife"));
		lifeTimeUserGroupInformation = kttf != null ? Long.parseLong(kttf) : 1000L*60*60*2;
	}

	@Override
	public String getName() {
		return name;
	}
	@Override
	public Properties getProperties() {
		return properties;
	}
	@Override
	public Configuration getConfiguration() {
		return configuration;
	}
	
	public void renewUserGroupInformation() throws IOException {
		if (SystemConfiguration.isSecurityEnabled()) {
			if (kerberosAuthenticator != null) {
				kerberosAuthenticator.relogin();
				userGroupInformation = SystemConfiguration.getUGIFromSubject(
					kerberosAuthenticator.getSubject());
			} else {
				userGroupInformation = SystemConfiguration.getLoginUser();
			}
		} else {
			userGroupInformation = SystemConfiguration.createRemoteUser(name);
		}
	}
	@Override
	public UserGroupInformation getUserGroupInformation() throws IOException {
		if (lastRenewUserGroupInformation + lifeTimeUserGroupInformation < System.currentTimeMillis()) {
			renewUserGroupInformation();
			lastRenewUserGroupInformation = System.currentTimeMillis();
		}
		return userGroupInformation;
	}

	@Override
	public void close() throws IOException {
		if (kerberosAuthenticator != null)
			kerberosAuthenticator.logout();
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getName());
		try {
			boolean hasKrb = getUserGroupInformation().hasKerberosCredentials();
			sb.append(" [hasKerberos=").append(hasKrb).append("]");
		} catch (IOException e) {
			// ignorar error
		}
		return sb.toString();
	}
}
