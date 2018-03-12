package infradoop.core.common.account;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public interface Account extends AutoCloseable {
	public static final String INHERIT = "inherit";
	public static final String DEFAULT = "default";
	
	public String getName();
	public Properties getProperties();
	public Configuration getConfiguration();
	public UserGroupInformation getUserGroupInformation() throws IOException;
	@Override
	public void close() throws IOException;
}
