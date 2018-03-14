package infradoop.core.common.account;

import infradoop.core.common.KerberosAuthenticator;
import infradoop.core.common.SystemConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class AccountManager {
	public static final Logger LOG = Logger.getLogger(AccountManager.class);
	private static final Map<String, Account> ACCOUNTS = new HashMap<>();
	
	public static void unregisterAll() {
		List<String> names = new ArrayList<>();
		names.addAll(ACCOUNTS.keySet());
		for (String name : names)
			unregister(name);
	}
	
	public static boolean unregister(Account account) {
		if (account == null)
			throw new IllegalArgumentException("account parameter can't be null");
		String name = null;
		for (Map.Entry<String, Account> entry: ACCOUNTS.entrySet())
			if (account.equals(entry.getValue())) {
				name = entry.getKey();
				break;
			}
		if (name != null) {
			ACCOUNTS.remove(name);
			try {
				account.close();
				LOG.debug("account closed ["+name+", "+account.getName()+"]");
				return true;
			} catch (IOException e) {
				LOG.warn("unable to close account ["+name+", "+account.getName()+"]", e);
				return false;
			}
		} else {
			return false;
		}
	}
	public static boolean unregister(String name) {
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("account parameter can't be empty");
		if (!ACCOUNTS.containsKey(name))
			return false;
		Account account = ACCOUNTS.remove(name);
		try {
			account.close();
			LOG.debug("account closed ["+name+", "+account.getName()+"]");
			return true;
		} catch (IOException e) {
			LOG.warn("unable to close account ["+name+", "+account.getName()+"]", e);
			return false;
		}
	}
	
	public static Account register(String name, Properties properties) throws IOException {
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("account parameter can't be empty");
		if (ACCOUNTS.containsKey(name))
			throw new IOException("account is already registered ["+name+"]");
		DefaultAccount account = new DefaultAccount(name, properties, null,
				SystemConfiguration.getCurrentUser(), SystemConfiguration.getConfiguration());
		ACCOUNTS.put(name, account);
		LOG.debug("account created ["+name+", "+account.getName()+"]");
		return account;
	}
	
	public static Account register(String name) throws IOException {
		return register(name, SystemConfiguration.getConfiguration());
	}
	public static Account register(String name, Configuration configuration) throws IOException {
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("account parameter can't be empty");
		if (ACCOUNTS.containsKey(name))
			throw new IOException("account is already registered ["+name+"]");
		UserGroupInformation ugi = SystemConfiguration.getLoginUser();
		//if (SystemConfiguration.isSecurityEnabled() && !ugi.hasKerberosCredentials())
		//	throw new AccountException("unable to create account ["+name+"], requiere kerberos ticket");
		DefaultAccount account = new DefaultAccount(ugi.getUserName(), new Properties(), null,
				ugi, configuration);
		ACCOUNTS.put(name, account);
		LOG.debug("account created ["+name+", "+account.getName()+"]");
		return account;
	}
	public static Account register(String name, String principal) throws IOException {
		return register(name, principal, null, null, SystemConfiguration.getConfiguration());
	}
	public static Account register(String name, String principal, Configuration configuration) throws IOException {
		return register(name, principal, null, null, configuration);
	}
	public static Account register(String name, String principal, File keytab) throws IOException {
		return register(name, principal, null, keytab, SystemConfiguration.getConfiguration());
	}
	public static Account register(String name, String principal, File keytab, Configuration configuration) throws IOException {
		return register(name, principal, null, keytab, configuration);
	}
	public static Account register(String name, String principal, String password) throws IOException {
		return register(name, principal, password, null, SystemConfiguration.getConfiguration());
	}
	public static Account register(String name, String principal, String password, Configuration configuration) throws IOException {
		return register(name, principal, password, null, configuration);
	}
	public static Account register(String name, String principal, String password, File keytab, Configuration configuration) throws IOException {
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("account parameter can't be empty");
		if (ACCOUNTS.containsKey(name))
			throw new IOException("account is already registered ["+name+"]");
		DefaultAccount account;
		if (SystemConfiguration.isSecurityEnabled() && (password != null || keytab != null)) {
			KerberosAuthenticator kauth = new KerberosAuthenticator(principal, password, keytab);
			account = new DefaultAccount(principal, new Properties(), kauth,
				null, configuration);
		} else {
			account = new DefaultAccount(principal, new Properties(), null,
					null, configuration);
		}
		account.renewUserGroupInformation();
		ACCOUNTS.put(name, account);
		LOG.debug("account created ["+name+", "+account.getName()+"]");
		return account;
	}
	
	public static Account get(String name) {
		return ACCOUNTS.get(name);
	}
}
