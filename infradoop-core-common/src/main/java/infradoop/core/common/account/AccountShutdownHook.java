package infradoop.core.common.account;

import infradoop.core.common.InfradoopShutdownHook;

public class AccountShutdownHook implements InfradoopShutdownHook {
	@Override
	public String getName() {
		return "Account-Shutdown-Hook";
	}
	@Override
	public float getPriority() {
		return 20f;
	}
	@Override
	public void run() {
		AccountManager.unregisterAll();
	}
}
