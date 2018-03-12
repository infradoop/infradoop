package infradoop.core.common.source;

import infradoop.core.common.entity.Grant;
import infradoop.core.common.entity.Nameable;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;
import java.io.IOException;

public interface ConnectorEntityHandler extends Connector {
	public String[] getDomains() throws IOException;
	public String[] getEntities(String domain) throws IOException;
	
	public EntityDescriptor buildEntityDescriptor(String entity) throws IOException;
	public EntityDescriptor buildEntityDescriptor(String domain, String entity) throws IOException;
	public EntityDescriptor buildEntityDescriptor(Nameable nameable) throws IOException;
	
	public void createEntity(EntityDescriptor entity) throws IOException;
	public void dropEntity(Nameable nameable) throws IOException;
	public boolean entityExists(Nameable nameable) throws IOException;
	
	public EntityWriter getEntityWriter(EntityDescriptor entityDesc, EntityWriterOptions options) throws IOException;
	
	public Grant[] retriveDomainGrants(String domain) throws IOException;
	public Grant[] retriveEntityGrants(String domain, String entity) throws IOException;
	public Grant[] retriveEntityGrants(Nameable nameable) throws IOException;
}
