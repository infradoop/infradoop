package infradoop.core.common.source;

import infradoop.core.common.entity.Grant;
import java.io.IOException;

public interface SecurityProvider extends AutoCloseable {
	public Grant[] retriveDomainGrants(String domain) throws IOException;
	public Grant[] retriveEntityGrants(String domain, String name) throws IOException;
	@Override
	void close() throws Exception;
}
