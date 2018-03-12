package infradoop.core.common.source;

import java.io.IOException;

public class Cdh5HiveSecureProviderFactory implements SecurityProviderFactory {
	@Override
	public boolean accept(Class<?> classType) {
		return classType.isAssignableFrom(Cdh5HiveConnector.class);
	}
	@Override
	public SecurityProvider create(Connector connector) throws IOException {
		return new Cdh5HiveSecureProvider(connector);
	}
}
