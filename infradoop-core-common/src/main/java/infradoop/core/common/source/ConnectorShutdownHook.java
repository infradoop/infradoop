package infradoop.core.common.source;

import infradoop.core.common.InfradoopShutdownHook;

public class ConnectorShutdownHook implements InfradoopShutdownHook {
	@Override
	public String getName() {
		return "Connector-Shutdown-Hook";
	}

	@Override
	public float getPriority() {
		return 10f;
	}

	@Override
	public void run() {
		ConnectorManager.closePool();
	}
}
