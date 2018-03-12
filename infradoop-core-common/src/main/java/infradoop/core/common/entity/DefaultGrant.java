package infradoop.core.common.entity;

public class DefaultGrant implements Grant {
	private final String grantee;
	private final GranteeType granteeType;
	private final Permission[] permissions;
	
	public DefaultGrant(String grantee, GranteeType granteeType, Permission[] permissions) {
		this.grantee = grantee;
		this.granteeType = granteeType;
		this.permissions = permissions;
	}

	@Override
	public String getGrantee() {
		return grantee;
	}
	@Override
	public GranteeType getGranteeType() {
		return granteeType;
	}
	@Override
	public Permission[] getPermissions() {
		return permissions;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(granteeType.name()+":"+grantee+":{");
		for (int i=0;i<getPermissions().length;i++) {
			if (i>0)
				sb.append(",");
			sb.append(getPermissions()[i].name());
		}
		sb.append("}");
		return sb.toString();
	}
}
