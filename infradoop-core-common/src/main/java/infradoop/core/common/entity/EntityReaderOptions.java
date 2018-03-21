package infradoop.core.common.entity;

import infradoop.core.common.data.DefinedVariables;

public class EntityReaderOptions {
	private int batchSize;
	private DefinedVariables variables;
	
	public EntityReaderOptions() {
		this.batchSize = 5000;
	}
	
	public EntityReaderOptions setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}
	public int getBatchSize() {
		return batchSize;
	}
	
	public DefinedVariables getVariables() {
		return variables;
	}
	public EntityReaderOptions setVariables(DefinedVariables variables) {
		this.variables = variables;
		return this;
	}
}
