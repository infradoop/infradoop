package infradoop.core.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import javax.security.auth.Subject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class SystemConfiguration extends Configuration {
	private static final Logger LOG = Logger.getLogger(SystemConfiguration.class);
	private static final List<InfradoopShutdownHook> SHUTDOWN_HOOKS = new ArrayList<>();
	
	static {
		for (InfradoopShutdownHook s : ServiceLoader.load(InfradoopShutdownHook.class))
			SHUTDOWN_HOOKS.add(s);
		Collections.sort(SHUTDOWN_HOOKS, new Comparator<InfradoopShutdownHook>() {
			@Override
			public int compare(InfradoopShutdownHook o1, InfradoopShutdownHook o2) {
				return Float.compare(o1.getPriority(), o2.getPriority());
			}
		});
		Runtime.getRuntime().addShutdownHook(new Thread() {
			{ setName("infradoop-shutdown"); }
			@Override
			public void run() {
				for (InfradoopShutdownHook s : SHUTDOWN_HOOKS) {
					LOG.debug("running ["+s.getName()+"]...");
					s.run();
				}
			}
		});
	}
	
	private static SystemConfiguration systemConfiguration;
	
	public static SystemConfiguration getConfiguration() {
		if (systemConfiguration == null) {
			systemConfiguration = new SystemConfiguration();
			File xmlFile;
			// cargar información de hadoop
			if ((xmlFile = new File(System.getProperty("hadoop.conf.path", "/etc/hadoop/conf"),
					"core-site.xml")).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
			}
			if ((xmlFile = new File(System.getProperty("hadoop.conf.path", "/etc/hadoop/conf"),
					"hdfs-site.xml")).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
			}
			// cargar información de hive
			if ((xmlFile = new File(System.getProperty("hive.conf.path", "/etc/hive/conf"),
					"hive-site.xml")).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
			}
			if ((xmlFile = new File(System.getProperty("hive.conf.path", "/etc/hive/conf"),
					"mapred-site.xml")).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
			}
			if (systemConfiguration.get("hive2.jdbc.url") == null && System.getProperty("hive2.jdbc.url") != null)
				systemConfiguration.set("hive2.jdbc.url", System.getProperty("hive2.jdbc.url"));
			// cargar información de hbase
			if ((xmlFile = new File(System.getProperty("hbase.conf.path", "/etc/hbase/conf"),
					"hbase-site.xml")).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
			}
			// cargar información de sentry
			if ((xmlFile = new File(System.getProperty("sentry.conf.path", "/etc/sentry/conf"),
					"sentry-site.xml")).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
				systemConfiguration.set("sentry.service.security.use.ugi", "false");
			}
			// cargar información de oozie
			if (System.getProperty("oozie.action.conf.xml") != null &&
					(xmlFile = new File(System.getProperty("oozie.action.conf.xml"))).exists()) {
				systemConfiguration.addResource(new Path(xmlFile.getAbsolutePath()));
				LOG.info(xmlFile.getAbsolutePath()+" configuration file loaded");
			}
			UserGroupInformation.setConfiguration(systemConfiguration);
		}
		return systemConfiguration;
	}
	
	public static String getProperty(String name) {
		return getConfiguration().get(name);
	}
	public static String getProperty(String name, String def) {
		return getConfiguration().get(name, def);
	}
	
	public static UserGroupInformation getLoginUser() {
		getConfiguration();
		try {
			return UserGroupInformation.getLoginUser();
		} catch (IOException e) {
			throw new RuntimeException("unable to get login user", e);
		}
	}
	public static UserGroupInformation createRemoteUser(String username) {
		getConfiguration();
		return UserGroupInformation.createRemoteUser(username);
	}
	public static UserGroupInformation getUGIFromSubject(Subject subject) {
		getConfiguration();
		try {
			return UserGroupInformation.getUGIFromSubject(subject);
		} catch (IOException e) {
			throw new RuntimeException("unable to get current user", e);
		}
	}
	public static UserGroupInformation getCurrentUser() {
		getConfiguration();
		try {
			return UserGroupInformation.getCurrentUser();
		} catch (IOException e) {
			throw new RuntimeException("unable to get current user", e);
		}
	}
	public static boolean isSecurityEnabled() {
		getConfiguration();
		return UserGroupInformation.isSecurityEnabled(); 
	}
	public static boolean isLoginKeytabBased() {
		getConfiguration();
		try {
			return UserGroupInformation.isLoginKeytabBased();
		} catch (IOException e) {
			throw new RuntimeException("unable to check if login from keytab is needed", e);
		} 
	}
	public static boolean isLoginTicketBased() {
		getConfiguration();
		try {
			return UserGroupInformation.isLoginTicketBased();
		} catch (IOException e) {
			throw new RuntimeException("unable to check if ticket is needed", e);
		} 
	}

	public static String getSystemRealm() {
		String systemRealm = systemConfiguration.get("dfs.namenode.kerberos.principal",
				systemConfiguration.get("dfs.datanode.kerberos.principal", 
						systemConfiguration.get("yarn.resourcemanager.principal")));
		if (systemRealm != null) { 
			if (systemRealm.contains("@"))
				systemRealm = systemRealm.substring(systemRealm.indexOf("@")+1);
		}
		return systemRealm;
	}
}
