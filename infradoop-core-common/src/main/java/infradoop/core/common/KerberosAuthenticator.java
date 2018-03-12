package infradoop.core.common;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class KerberosAuthenticator {
	private final String principal;
	private final String password;
	private final File keytab;
	private Subject subject;
	private LoginContext loginContext;
	
	public KerberosAuthenticator(String principal)
			throws IOException {
		this(principal, null, null);
	}
	public KerberosAuthenticator(String principal, File keytab)
			throws IOException {
		this(principal, null, keytab);
	}
	public KerberosAuthenticator(String principal, String password)
			throws IOException {
		this(principal, password, null);
	}
	public KerberosAuthenticator(String principal, String password, File keytab)
			throws IOException {
		this.principal = principal;
		this.password = password;
		this.keytab = keytab;
		relogin();
	}
	
	public void relogin() throws IOException {
		// cerrar sesion anterior
		logout();
		// iniciar nueva sesión
		subject = new Subject();
		try {
			loginContext = new LoginContext(
					principal, subject, null,
					new KerberosConfiguration(principal, keytab));
		} catch (LoginException e) {
			throw new IOException("unable to create login context ["+principal+"]", e);
		}
		if (password != null) {
			try {
				HashMap<String, Object> stateMap = new HashMap<>();
				stateMap.put("javax.security.auth.login.password", password.toCharArray());
				Field stateField = LoginContext.class.getDeclaredField("state");
				if (!stateField.isAccessible()) stateField.setAccessible(true);
				stateField.set(loginContext, stateMap);
			} catch (IllegalAccessException | NoSuchFieldException | SecurityException  e) {
				throw new IOException("unable to set password from ticket renewer ["+principal+"]", e);
			}
		}
		try {
			loginContext.login();
		} catch (LoginException e) {
			throw new IOException("unable to login context ["+principal+"]", e);
		}
	}
	
	public Subject getSubject() {
		return subject;
	}
	public void logout() throws IOException {
		if (loginContext != null)
			try {
				loginContext.logout();
			} catch (LoginException e) {
				throw new IOException("unable to logout from kerberos autenticator ["+principal+"]", e);
			}
	}
	
	public <T> T doAs(final PrivilegedExceptionAction<T> action)
            throws PrivilegedActionException {
		return Subject.doAs(subject, action);
	}
	
	private static class KerberosConfiguration extends Configuration {
		private static String getKrb5LoginModuleName() {
			return System.getProperty("java.vendor").contains("IBM") ?
					"com.ibm.security.auth.module.Krb5LoginModule"
						: "com.sun.security.auth.module.Krb5LoginModule";
		}
		private final String principal;
		private final String keytab;
		
		public KerberosConfiguration(String principal, File keytab) throws LoginException {
			this.principal = principal;
			if (keytab != null) {
				if (!keytab.exists())
					throw new LoginException("keytab file "+keytab.getAbsolutePath()+" not exists");
				try {
					this.keytab = keytab.getCanonicalPath();
				} catch (IOException e) {
					throw new LoginException("unable to get canonical path from keytab");
				}
			} else {
				this.keytab = null;
			}
		}
		
		@Override
		public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
			Map<String, String> options = new HashMap<>();
			options.put("refreshKrb5Config", "true");
			options.put("principal", principal);
			options.put("doNotPrompt", "true");
			options.put("isInitiator", "true");
			options.put("useTicketCache", "true");
			if (keytab != null) {
				options.put("keyTab", keytab);
				options.put("useKeyTab", "true");
				options.put("storeKey", "true");
			} else {
				options.put("useFirstPass", "true");
			}
			return new AppConfigurationEntry[] {
					new AppConfigurationEntry(getKrb5LoginModuleName(),
							AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
					};
		}
	}
}
