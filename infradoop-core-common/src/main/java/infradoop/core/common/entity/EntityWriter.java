package infradoop.core.common.entity;

import java.io.IOException;

import infradoop.core.common.source.Connector;

public abstract class EntityWriter implements AutoCloseable {
	protected final Connector connector;
	protected final EntityDescriptor entity;
	protected final EntityWriterOptions options;
	
	public EntityWriter(Connector connector, EntityDescriptor entity, EntityWriterOptions options) {
		this.connector = connector;
		this.entity = entity;
		this.options = options;
	}
	
	public abstract void initialize() throws IOException;
	public abstract EntityWriter set(int index, String value) throws IOException;
	public abstract EntityWriter set(int index, Object value) throws IOException;
	public abstract EntityWriter write() throws IOException;
	public abstract void uninitialize() throws IOException;
	
	@Override
	public void close() throws IOException {
		uninitialize();
	}
}
