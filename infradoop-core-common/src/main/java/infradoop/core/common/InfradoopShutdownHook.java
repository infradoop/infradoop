package infradoop.core.common;

public interface InfradoopShutdownHook {
	public String getName();
	public float getPriority();
	public void run();
}
