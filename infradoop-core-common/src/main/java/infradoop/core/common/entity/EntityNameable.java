package infradoop.core.common.entity;

public class EntityNameable implements Nameable {
	private String domain;
	private String name;
	
	public EntityNameable(String name) {
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("entity name cannot be empty");
		if (name.contains(".")) {
			String n[] = name.split("\\.");
			initialize(n[0], n[1]);
		} else {
			initialize(null, name);
		}
	}
	public EntityNameable(String domain, String name) {
		initialize(domain, name);
	}
	
	protected void initialize(String domain, String name) {
		if (domain != null) {
			if ("".equals(domain))
				throw new IllegalArgumentException("entity domain cannot be empty");
			if (!domain.matches("[_a-zA-Z][_a-zA-Z0-9]*"))
				throw new IllegalArgumentException("domain \""+name+"\" is invalid");
		}
		this.domain = domain;
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("entity name cannot be empty");
		if (!name.matches("[_a-zA-Z][_a-zA-Z0-9\\.]*"))
			throw new IllegalArgumentException("name \""+name+"\" is invalid");
		this.name = name;
	}
	
	@Override
	public String getDomain() {
		return domain;
	}
	@Override
	public String getName() {
		return name;
	}
	@Override
	public String getCanonicalName() {
		if (getDomain() == null)
			return getName();
		else
			return getDomain()+"."+getName();
	}
}
