package infradoop.core.common.source;

import infradoop.core.common.entity.DefaultGrant;
import infradoop.core.common.entity.Grant;
import infradoop.core.common.entity.GranteeType;
import infradoop.core.common.entity.Permission;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;

public class Cdh5HbaseSecureProvider implements SecurityProvider {
	protected final Connector connector;
	
	public Cdh5HbaseSecureProvider(Connector connector) {
		this.connector = connector;
	}
	
	private Permission toPermission(Action action) {
		switch (action) {
		case ADMIN:
			return Permission.ADMIN;
		case CREATE:
			return Permission.CREATE;
		case EXEC:
			return Permission.EXEC;
		case READ:
			return Permission.READ;
		case WRITE:
			return Permission.WRITE;
		default:
			throw new IllegalArgumentException("unsupported permission "+action.name());
		}
	}
	
	@Override
	public Grant[] retriveDomainGrants(String domain) throws IOException {
		try {
			Map<String, Set<Permission>> permissionsGroup = new HashMap<>();
			List<UserPermission> permissions;
			// get permissions from namespace
			permissions = AccessControlClient.getUserPermissions(
					(Connection)connector.unwrap(), "@"+domain);
			for (UserPermission p : permissions) {
				String grantee = new String(p.getUser());
				Set<Permission> perms = permissionsGroup.get(grantee);
				if (perms == null)
					permissionsGroup.put(grantee, perms = new HashSet<>());
				for (Action a : p.getActions()) {
					perms.add(toPermission(a));
				}
			}
			
			// create grant struct
			Grant[] grants = new Grant[permissionsGroup.size()];
			int i=0;
			for (String grantee : permissionsGroup.keySet()) {
				Set<Permission> perms = permissionsGroup.get(grantee);
				GranteeType granteeType;
				if (grantee.startsWith("@")) {
					grantee = grantee.substring(1);
					granteeType = GranteeType.GROUP;
				} else {
					granteeType = GranteeType.USER;
				}
				grants[i++] = new DefaultGrant(grantee, granteeType, perms.toArray(new Permission[perms.size()]));
			}
			return grants;
		} catch (Throwable e) {
			throw new IOException("unable to get domain grants "
					+ "["+domain+", "+connector.getAccount().getName()+"]", e);
		}
	}
	
	@Override
	public Grant[] retriveEntityGrants(String domain, String entity) throws IOException {
		try {
			Map<String, Set<Permission>> permissionsGroup = new HashMap<>();
			List<UserPermission> permissions;
			// get permissions from namespace
			permissions = AccessControlClient.getUserPermissions(
					(Connection)connector.unwrap(), "@"+domain);
			for (UserPermission p : permissions) {
				String grantee = new String(p.getUser());
				Set<Permission> perms = permissionsGroup.get(grantee);
				if (perms == null)
					permissionsGroup.put(grantee, perms = new HashSet<>());
				for (Action a : p.getActions()) {
					perms.add(toPermission(a));
				}
			}
			// get permissions from table
			permissions = AccessControlClient.getUserPermissions(
					(Connection)connector.unwrap(), "@"+domain+":"+entity);
			for (UserPermission p : permissions) {
				String grantee = new String(p.getUser());
				Set<Permission> perms = permissionsGroup.get(grantee);
				if (perms == null)
					permissionsGroup.put(grantee, perms = new HashSet<>());
				for (Action a : p.getActions()) {
					perms.add(toPermission(a));
				}
			}
			
			// create grant struct
			Grant[] grants = new Grant[permissionsGroup.size()];
			int i=0;
			for (String grantee : permissionsGroup.keySet()) {
				Set<Permission> perms = permissionsGroup.get(grantee);
				GranteeType granteeType;
				if (grantee.startsWith("@")) {
					grantee = grantee.substring(1);
					granteeType = GranteeType.GROUP;
				} else {
					granteeType = GranteeType.USER;
				}
				grants[i++] = new DefaultGrant(grantee, granteeType, perms.toArray(new Permission[perms.size()]));
			}
			return grants;
		} catch (Throwable e) {
			throw new IOException("unable to get entity grants "
					+ "["+domain+"."+entity+", "+connector.getAccount().getName()+"]", e);
		}
	}
	
	@Override
	public void close() throws Exception {
	}
}
