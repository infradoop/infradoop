package infradoop.core.common.entity;

import infradoop.core.common.data.DefinedVariables;

public class EntityWriterOptions {
	private int batchSize;
	private DefinedVariables variables;
	
	public EntityWriterOptions() {
		this.batchSize = 5000;
	}
	
	public EntityWriterOptions setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}
	public int getBatchSize() {
		return batchSize;
	}
	
	public DefinedVariables getVariables() {
		return variables;
	}
	public EntityWriterOptions setVariables(DefinedVariables dynamicValues) {
		this.variables = dynamicValues;
		return this;
	}
}
