package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;

public abstract class HbaseConnector extends AbstractConnectorEntityHandler {
	public HbaseConnector(Account account, Object connection) {
		super(account, connection);
	}
	
	@Override
	public String getConnectorType() {
		return "hbase";
	}
	
	public void abort(String why, Throwable e) {
		((Connection)connection).abort(why, e);
	}
	public Admin getAdmin() throws IOException {
		return ((Connection)connection).getAdmin();
	}
	public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
		return ((Connection)connection).getBufferedMutator(params);
	}
	public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
		return ((Connection)connection).getBufferedMutator(tableName);
	}
	public Configuration getConfiguration() {
		return ((Connection)connection).getConfiguration();
	}
	public RegionLocator getRegionLocator(TableName tableName) throws IOException {
		return ((Connection)connection).getRegionLocator(tableName);
	}
	public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
		return ((Connection)connection).getTable(tableName, pool);
	}
	public Table getTable(TableName tableName) throws IOException {
		return ((Connection)connection).getTable(tableName);
	}
	public boolean isAborted() {
		return ((Connection)connection).isAborted();
	}
	public boolean isClosed() {
		return ((Connection)connection).isClosed();
	}

	@Override
	public void close() throws IOException {
		try {
			super.close();
		} catch (Exception e) {
			throw new IOException("unable to close connector "
					+ "["+getConnectorType()+", "+getAccount().getName()+"]", e);
		}
	}
}
