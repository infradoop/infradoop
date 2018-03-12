package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.fs.FileSystem;

public class Cdh5HdfsConnectorFactory implements ConnectorFactory {
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(Cdh5HdfsConnector.class);
	}
	
	@Override
	public Connector create(final Account account) throws Exception {
		FileSystem fs = account.getUserGroupInformation().doAs(
				new PrivilegedExceptionAction<FileSystem>() {
					@Override
					public FileSystem run() throws Exception {
						return FileSystem.get(account.getConfiguration());
					}
				});
		return new Cdh5HdfsConnector(account, fs);
	}
}
