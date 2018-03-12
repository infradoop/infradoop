package infradoop.core.common.entity;

public interface Grant {
	public String getGrantee();
	public GranteeType getGranteeType();
	public Permission[] getPermissions();
}
