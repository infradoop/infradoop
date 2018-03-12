package infradoop.core.common.source;

import java.io.IOException;

public interface SecurityProviderFactory {
	public boolean accept(Class<?> classType);
	public SecurityProvider create(Connector connector) throws IOException;
}
