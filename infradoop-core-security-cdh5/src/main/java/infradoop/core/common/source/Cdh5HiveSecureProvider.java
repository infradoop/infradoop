package infradoop.core.common.source;

import infradoop.core.common.entity.DefaultGrant;
import infradoop.core.common.entity.Grant;
import infradoop.core.common.entity.GranteeType;
import infradoop.core.common.entity.Permission;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;

public class Cdh5HiveSecureProvider implements SecurityProvider {
	protected final Connector connector;
	private SentryPolicyServiceClient sentry;
	private Set<TSentryRole> roles;
	private Map<String, Set<TSentryPrivilege>> privilegesMap;
	
	public Cdh5HiveSecureProvider(final Connector connector) throws IOException {
		this.connector = connector;
		try {
			sentry = connector.getAccount().getUserGroupInformation().doAs(
					new PrivilegedExceptionAction<SentryPolicyServiceClient>() {
						@Override
						public SentryPolicyServiceClient run() throws Exception {
							return SentryServiceClientFactory.create(
									connector.getAccount().getConfiguration());
						}
					});
		} catch (InterruptedException e) {
			throw new IOException("unable to create sentry client", e);
		}
	}
	
	protected Set<TSentryRole> getRoles() throws SentryUserException, IOException {
		if (roles == null) {
			String userName = connector.getAccount().getUserGroupInformation().getUserName();
			roles = sentry.listRoles(userName);
		}
		return roles;
	}
	protected Set<TSentryPrivilege> getAllPrivilegesByRole(String roleName) throws SentryUserException, IOException {
		if (privilegesMap == null)
			privilegesMap = new HashMap<>();
		Set<TSentryPrivilege> privilages = privilegesMap.get(roleName);
		if (privilages == null) {
			String userName = connector.getAccount().getUserGroupInformation().getUserName();
			privilegesMap.put(roleName, privilages = sentry.listAllPrivilegesByRoleName(userName, roleName));
		}
		return privilages;
	}
	
	@Override
	public Grant[] retriveDomainGrants(String domain) throws IOException {
		try {
			Map<String, Set<Permission>> permissionMap = new HashMap<>();
			for (TSentryRole role : getRoles()) {
				if (role.getRoleName() != null && !"".equals(role.getRoleName())) {
					for (TSentryGroup group : role.getGroups()) {
						Set<Permission> permissionSetByGroup = permissionMap.get(group.getGroupName());
						if (permissionSetByGroup == null)
							permissionMap.put(group.getGroupName(), permissionSetByGroup = new HashSet<>());
						for (TSentryPrivilege priv : getAllPrivilegesByRole(role.getRoleName())) {
							Permission permissions[];
							if ("*".equals(priv.getAction()) || "all".equalsIgnoreCase(priv.getAction()))
								permissions = new Permission[] {Permission.READ, Permission.WRITE};
							else if ("insert".equalsIgnoreCase(priv.getAction()))
								permissions = new Permission[] {Permission.WRITE};
							else if ("select".equalsIgnoreCase(priv.getAction()))
								permissions = new Permission[] {Permission.READ};
							else
								permissions = new Permission[0];
							if ("SERVER".equals(priv.getPrivilegeScope())) {
								permissionSetByGroup.addAll(Arrays.asList(permissions));
							} else if ("DATABASE".equals(priv.getPrivilegeScope())
									&& domain.equals(priv.getDbName())) {
								permissionSetByGroup.addAll(Arrays.asList(permissions));
							}
						}
					}
				}
			}
			
			// create grant struct
			List<Grant> grants = new ArrayList<>();
			for (String grantee : permissionMap.keySet()) {
				Set<Permission> perms = permissionMap.get(grantee);
				if (perms.size() > 0)
					grants.add(new DefaultGrant(grantee, GranteeType.GROUP,
							perms.toArray(new Permission[perms.size()])));
			}
			return grants.toArray(new Grant[grants.size()]);
		} catch (SentryUserException e) {
			throw new IOException("unable to read domain grants "
					+ "["+domain+", "+connector.getAccount().getName()+"]", e);
		}
	}
	
	@Override
	public Grant[] retriveEntityGrants(String domain, String entity) throws IOException {
		try {
			Map<String, Set<Permission>> permissionMap = new HashMap<>();
			for (TSentryRole role : getRoles()) {
				if (role.getRoleName() != null && !"".equals(role.getRoleName())) {
					for (TSentryGroup group : role.getGroups()) {
						Set<Permission> permissionSetByGroup = permissionMap.get(group.getGroupName());
						if (permissionSetByGroup == null)
							permissionMap.put(group.getGroupName(), permissionSetByGroup = new HashSet<>());
						for (TSentryPrivilege priv : getAllPrivilegesByRole(role.getRoleName())) {
							Permission permissions[];
							if ("*".equals(priv.getAction()) || "all".equalsIgnoreCase(priv.getAction()))
								permissions = new Permission[] {Permission.READ, Permission.WRITE};
							else if ("insert".equalsIgnoreCase(priv.getAction()))
								permissions = new Permission[] {Permission.WRITE};
							else if ("select".equalsIgnoreCase(priv.getAction()))
								permissions = new Permission[] {Permission.READ};
							else
								permissions = new Permission[0];
							if ("SERVER".equalsIgnoreCase(priv.getPrivilegeScope())) {
								permissionSetByGroup.addAll(Arrays.asList(permissions));
							} else if ("DATABASE".equalsIgnoreCase(priv.getPrivilegeScope())
									&& domain.equals(priv.getDbName())) {
								permissionSetByGroup.addAll(Arrays.asList(permissions));
							} else if ("TABLE".equalsIgnoreCase(priv.getPrivilegeScope())
									&& domain.equals(priv.getDbName()) && entity.equals(priv.getTableName())) {
								permissionSetByGroup.addAll(Arrays.asList(permissions));
							}
						}
					}
				}
			}
			
			// create grant struct
			List<Grant> grants = new ArrayList<>();
			for (String grantee : permissionMap.keySet()) {
				Set<Permission> perms = permissionMap.get(grantee);
				if (perms.size() > 0)
					grants.add(new DefaultGrant(grantee, GranteeType.GROUP,
							perms.toArray(new Permission[perms.size()])));
			}
			return grants.toArray(new Grant[grants.size()]);
		} catch (SentryUserException e) {
			throw new IOException("unable to read domain grants "
					+ "["+domain+", "+connector.getAccount().getName()+"]", e);
		}
	}
	
	@Override
	public void close() throws Exception {
		if (roles != null)
			roles.clear();
		if (privilegesMap != null)
			privilegesMap.clear();
		sentry.close();
	}
}
