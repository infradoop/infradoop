package infradoop.core.common.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import infradoop.core.common.account.Account;

public abstract class HdfsConnector extends FileSystem implements Connector {
	private static final Logger LOG = Logger.getLogger(HdfsConnector.class);
	
	protected final Account account;
	protected final FileSystem fileSystem;
	
	private String poolKey;
	private KeyedObjectPool<String, Connector> pool;
	
	public HdfsConnector(Account account, FileSystem fileSystem) {
		this.account = account;
		this.fileSystem = fileSystem;
		setConf(account.getConfiguration());
		LOG.debug("connector created ["+getConnectorType()+", "+account.getName()+"]");
	}
	
	@Override
	public String getConnectorType() {
		return "hdfs";
	}
	@Override
	public Account getAccount() {
		return account;
	}
	@Override
	public Object unwrap() {
		return fileSystem;
	}
	
	@Override
	public void setPool(String poolKey, KeyedObjectPool<String, Connector> pool) {
		this.poolKey = poolKey;
		this.pool = pool;
	}
	@Override
	public String getPoolKey() {
		return poolKey;
	}
	@Override
	public KeyedObjectPool<String, Connector> getPool() {
		return pool;
	}
	
	@Override
	public URI getUri() {
		return fileSystem.getUri();
	}
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		return fileSystem.open(f, bufferSize);
	}
	@Override
	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress) throws IOException {
		return fileSystem.create(f, overwrite, bufferSize, replication, blockSize, progress);
	}
	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
		return fileSystem.append(f, bufferSize, progress);
	}
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		return fileSystem.rename(src, dst);
	}
	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		return fileSystem.delete(f, recursive);
	}
	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
		return fileSystem.listStatus(f);
	}
	@Override
	public void setWorkingDirectory(Path new_dir) {
		fileSystem.setWorkingDirectory(new_dir);
	}
	@Override
	public Path getWorkingDirectory() {
		return fileSystem.getWorkingDirectory();
	}
	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		return fileSystem.mkdirs(f, permission);
	}
	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		return fileSystem.getFileStatus(f);
	}
	
	@Override
	public void close() throws IOException {
		if (getPool() == null) {
			super.close();
			fileSystem.close();
			LOG.debug("connector closed ["+getConnectorType()+", "+account.getName()+"]");
		} else {
			try {
				getPool().returnObject(getPoolKey(), this);
			} catch (Exception e) {
				throw new IOException("unable to return hdfs connector to pool ["+getAccount().getName()+"]", e);
			}
		}
	}
}
