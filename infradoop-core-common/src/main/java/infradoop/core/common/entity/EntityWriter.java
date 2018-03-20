package infradoop.core.common.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import infradoop.core.common.data.VariableParser;
import infradoop.core.common.source.Connector;

public abstract class EntityWriter implements EntityRow, AutoCloseable {
	protected final Connector connector;
	protected final EntityDescriptor entityDescriptor;
	protected final EntityWriterOptions writerOptions;
	private final EntityRowVariableResolver variableResolver;
	private final List<Integer> dynamicAttributes;
	
	public EntityWriter(Connector connector, EntityDescriptor entityDescriptor, EntityWriterOptions writerOptions) {
		this.connector = connector;
		this.entityDescriptor = entityDescriptor;
		this.writerOptions = writerOptions;
		this.dynamicAttributes = new ArrayList<>();
		for (int i=0;i<entityDescriptor.countAttributes();i++) {
			Attribute attr = entityDescriptor.getAttribute(i);
			if (attr.hasDynamicValue())
				dynamicAttributes.add(i);
		}
		variableResolver = new EntityRowVariableResolver(writerOptions.getVariables());
	}
	
	public abstract void initialize() throws IOException;
	public abstract void write() throws IOException;
	public abstract void uninitialize() throws IOException;
	
	@Override
	public Attribute getAttribute(int index) {
		return entityDescriptor.getAttribute(index);
	}
	@Override
	public Attribute getAttribute(String name) {
		return entityDescriptor.getAttribute(name);
	}
	@Override
	public int indexOfAttribute(String name) {
		return entityDescriptor.indexOfAttribute(name);
	}
	@Override
	public int countAttributes() {
		return entityDescriptor.countAttributes();
	}
	
	protected void evaluateDynamicValues() throws IOException {
		if (!dynamicAttributes.isEmpty()) {
			variableResolver.setCurrent(this);
			for (int index : dynamicAttributes)
				setValue(index, VariableParser.parse(getAttribute(index).getDynamicValue(), variableResolver));
		}
	}
	
	@Override
	public void close() throws IOException {
		uninitialize();
	}
}
