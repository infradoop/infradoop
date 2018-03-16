package infradoop.core.common.entity;

import java.text.SimpleDateFormat;

public class Attribute {
	private final EntityDescriptor entity;
	private final String name;
	private final byte[] nameAsByteArray;
	private final DataType type;
	private String family;
	private byte[] familyAsByteArray;
	private int start;
	private int end;
	private int decimal;
	private boolean hasPath;
	private String path;
	private boolean hasDynamicValue;
	private String dynamicValue;
	private boolean required;
	private boolean hidden;
	private boolean indexable;
	private SimpleDateFormat dateFormat;
	private boolean hasStaticValue;
	private String staticValue;
	
	Attribute(EntityDescriptor entity, String name, DataType type) {
		this.entity = entity;
		this.name = name;
		this.nameAsByteArray = name.getBytes();
		this.type = type;
		family = "default";
		familyAsByteArray = family.getBytes();
		required = false;
		hidden = false;
		indexable = true;
		hasPath = false;
		hasDynamicValue = false;
		hasStaticValue = false;
	}
	
	public EntityDescriptor getEntity() {
		return entity;
	}
	public String getName() {
		return name;
	}
	public byte[] getNameAsByteArray() {
		return nameAsByteArray;
	}
	public DataType getType() {
		return type;
	}
	public String getFamily() {
		return family;
	}
	public byte[] getFamilyAsByteArray() {
		return familyAsByteArray;
	}
	public Attribute setFamily(String family) {
		this.family = family;
		familyAsByteArray = family.getBytes();
		return this;
	}
	public boolean isRequired() {
		return required;
	}
	public Attribute setRequired(boolean required) {
		this.required = required;
		return this;
	}
	public boolean isHidden() {
		return hidden;
	}
	public Attribute setHidden(boolean hidden) {
		this.hidden = hidden;
		return this;
	}
	public SimpleDateFormat getDateFormat() {
		return dateFormat;
	}
	public Attribute setDateFormat(SimpleDateFormat dateFormat) {
		this.dateFormat = dateFormat;
		return this;
	}
	public String getStaticValue() {
		return staticValue;
	}
	public Attribute setStaticValue(String staticValue) {
		this.staticValue = staticValue;
		hasStaticValue = true;
		return this;
	}
	public boolean hasStaticValue() {
		return hasStaticValue;
	}
	public int getStart() {
		return start;
	}
	public Attribute setStart(int start) {
		this.start = start;
		return this;
	}
	public int getEnd() {
		return end;
	}
	public Attribute setEnd(int end) {
		this.end = end;
		return this;
	}
	public int getDecimal() {
		return decimal;
	}
	public Attribute setDecimal(int decimal) {
		this.decimal = decimal;
		return this;
	}
	public boolean isIndexable() {
		return indexable;
	}
	public Attribute setIndexable(boolean indexable) {
		this.indexable = indexable;
		return this;
	}
	public String getDynamicValue() {
		return dynamicValue;
	}
	public Attribute setDynamicValue(String reference) {
		this.dynamicValue = reference;
		hasDynamicValue = true;
		return this;
	}
	public boolean hasDynamicValue() {
		return hasDynamicValue;
	}
	public String getPath() {
		return path;
	}
	public Attribute setPath(String path) {
		this.path = path;
		hasPath = true;
		return this;
	}
	public boolean hasPath() {
		return hasPath;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (getFamily() != null)
			sb.append(getFamily()).append(":");
		sb.append(getName()).append(" ").append(getType().name());
		if (getStart() > 0)
			sb.append(" ").append(getStart()).append(getEnd());
		if (isRequired())
			sb.append(" requiered=").append(isRequired());
		if (!isIndexable())
			sb.append(" indexable=").append(isIndexable());
		return sb.toString();
	}
}
