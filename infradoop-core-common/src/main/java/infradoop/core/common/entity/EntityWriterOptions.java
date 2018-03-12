package infradoop.core.common.entity;

public class EntityWriterOptions {
	private int batchSize;
	
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
}
