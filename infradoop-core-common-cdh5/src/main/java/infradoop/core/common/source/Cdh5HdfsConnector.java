package infradoop.core.common.source;

import infradoop.core.common.account.Account;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;

public class Cdh5HdfsConnector extends HdfsConnector {
	public Cdh5HdfsConnector(Account account, FileSystem fileSystem) {
		super(account, fileSystem);
	}
	
	@Override
	public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
		fileSystem.modifyAclEntries(path, aclSpec);
	}
	@Override
	public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
		fileSystem.removeAclEntries(path, aclSpec);
	}
	@Override
	public void removeDefaultAcl(Path path) throws IOException {
		fileSystem.removeDefaultAcl(path);
	}
	@Override
	public AclStatus getAclStatus(Path path) throws IOException {
		return fileSystem.getAclStatus(path);
	}
	@Override
	public void removeAcl(Path path) throws IOException {
		fileSystem.removeAcl(path);
	}
	@Override
	public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
		fileSystem.setAcl(path, aclSpec);
	}
}
