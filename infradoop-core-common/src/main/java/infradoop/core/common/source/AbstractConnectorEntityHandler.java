package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;
import infradoop.core.common.entity.Grant;
import infradoop.core.common.entity.Nameable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public abstract class AbstractConnectorEntityHandler extends AbstractConnector implements ConnectorEntityHandler {
	private static List<SecurityProviderFactory> securityProviders;
	
	private static SecurityProviderFactory getSecureProvider(Class<?> classType) {
		for (SecurityProviderFactory sp : getSecurityProviders())
			if (sp.accept(classType))
				return sp;
		return null;
	}
	private static List<SecurityProviderFactory> getSecurityProviders() {
		if (securityProviders == null) {
			securityProviders = new ArrayList<>();
			for (SecurityProviderFactory sp : ServiceLoader.load(SecurityProviderFactory.class))
				securityProviders.add(sp);
		}
		return securityProviders;
	}
	
	private SecurityProvider securityProvider;
	
	public AbstractConnectorEntityHandler(Account account, Object connection) {
		super(account, connection);
	}
	
	protected SecurityProvider getSecurityProvider() throws IOException {
		if (securityProvider == null) {
			SecurityProviderFactory securityProviderFactory = getSecureProvider(getClass());
			if (securityProviderFactory != null)
				securityProvider = securityProviderFactory.create(this);
		}
		return securityProvider;
	}
	
	@Override
	public Grant[] retriveDomainGrants(String domain) throws IOException {
		return getSecurityProvider().retriveDomainGrants(domain);
	}
	@Override
	public Grant[] retriveEntityGrants(String domain, String entity) throws IOException {
		return getSecurityProvider().retriveEntityGrants(domain, entity);
	}
	@Override
	public Grant[] retriveEntityGrants(Nameable nameable) throws IOException {
		return getSecurityProvider().retriveEntityGrants(nameable.getDomain(), nameable.getName());
	}
	
	public EntityWriter getEntityWriter(EntityDescriptor entityDesc) throws IOException {
		return getEntityWriter(entityDesc, new EntityWriterOptions());
	}
}
