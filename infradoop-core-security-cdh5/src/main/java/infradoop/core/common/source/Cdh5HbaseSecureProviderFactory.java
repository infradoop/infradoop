package infradoop.core.common.source;

import java.io.IOException;

public class Cdh5HbaseSecureProviderFactory implements SecurityProviderFactory {
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(Cdh5HbaseConnector.class);
	}
	@Override
	public SecurityProvider create(Connector connector) throws IOException {
		return new Cdh5HbaseSecureProvider(connector);
	}
}
