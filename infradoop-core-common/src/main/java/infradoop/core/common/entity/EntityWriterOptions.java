package infradoop.core.common.entity;

import java.util.Map;

public class EntityWriterOptions {
	private int batchSize;
	private Map<String, String> variables;
	
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
	
	public Map<String, String> getVariables() {
		return variables;
	}
	public EntityWriterOptions setVariables(Map<String, String> dynamicValues) {
		this.variables = dynamicValues;
		return this;
	}
}
