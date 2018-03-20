package infradoop.core.common.entity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

public class EntityDescriptor extends EntityNameable {
	private List<Attribute> attributes;
	private Map<String, Integer> attributesIndexes;
	private boolean dynamics;
	private int shards;
	private String directory;

	public EntityDescriptor(Nameable nameable) {
		super(nameable.getDomain(), nameable.getName());
	}
	public EntityDescriptor(String name) {
		super(name);
	}
	public EntityDescriptor(String domain, String name) {
		super(domain, name);
	}
	
	@Override
	protected void initialize(String domain, String name) {
		super.initialize(domain, name);
		attributes = new ArrayList<>();
		attributesIndexes = new HashMap<>();
		dynamics = false;
		shards = 1;
	}
	
	public int countAttributes() {
		return attributes.size();
	}
	public Attribute getAttribute(int index) {
		return attributes.get(index);
	}
	public Attribute getAttribute(String name) {
		int index = indexOfAttribute(name);
		if (index < 0)
			return null;
		return attributes.get(index);
	}
	public int indexOfAttribute(String name) {
		Integer index = attributesIndexes.get(name);
		if (index == null)
			return -1;
		else
			return index;
	}
	public String[] getAttributesNames() {
		String[] names = new String[attributes.size()];
		for (int i=0;i<attributes.size();i++)
			names[i] = attributes.get(i).getName();
		return names;
	}
	public boolean hasAttribute(String name) {
		return getAttribute(name) != null;
	}
	public EntityDescriptor compileAttributes(String code) {
		if (code == null || "".equals(code))
			throw new IllegalArgumentException("unable to compile empty string attribute");
		Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", Pattern.MULTILINE);
		for (String codeAttribute : pattern.split(code)) {
			compileAttribute(codeAttribute);
		}
		return this;
	}
	public EntityDescriptor compileAttribute(String code) {
		if (code == null || "".equals(code))
			throw new IllegalArgumentException("unable to compile empty string attribute");
		Pattern pattern = Pattern.compile(
				"[ \t\n\r]+(?=([^\"]*\"[^\"]*\")*[^\"]*$)", Pattern.MULTILINE);
		LinkedList<String> tokens = new LinkedList<>();
		for (String token : pattern.split(code.trim())) {
			if (token.startsWith("\"") && token.endsWith("\""))
				token = token.substring(1, token.length()-1);
			tokens.add(token);
		}
		if (tokens.size() < 2)
			throw new IllegalArgumentException("invalid attribute definition \""
					+code+"\" for entity"+getCanonicalName());
		String name = tokens.pop();
		String family = "default";
		if (name.contains(":")) {
			String p[] = name.split("\\:", 2);
			family = p[0];
			name = p[1];
		}
		DataType type = DataType.valueOf(tokens.pop().toUpperCase());
		Attribute attribute = addAttribute(name, type);
		attribute.setFamily(family);
		if (!tokens.isEmpty() && (type == DataType.TIMESTAMP || type == DataType.DATE)) {
			String dateFormat = null;
			try {
				attribute.setDateFormat(new SimpleDateFormat(dateFormat = tokens.pop()));
			} catch (Exception e) {
				tokens.push(dateFormat);
			}
		}
		if (!tokens.isEmpty()) {
			String value = tokens.pop();
			if ("=".equals(value) || "set".equals(value)) {
				if (tokens.isEmpty())
					throw new IllegalArgumentException(
							"invalid set attribute \""+name+"\" value "
									+ "for entity "+getCanonicalName());
				attribute.setDynamicValue(StringEscapeUtils.unescapeJava(tokens.pop()));
			} else if ("path".equals(value)) {
				if (tokens.isEmpty())
					throw new IllegalArgumentException(
							"invalid set attribute \""+name+"\" value "
									+ "for entity "+getCanonicalName());
				attribute.setPath(StringEscapeUtils.unescapeJava(tokens.pop()));
			} else if (value.matches("\\d+:\\d+(.\\d+)?")) {
				String pos[] = value.split("\\:", 2);
				attribute.setStart(Integer.parseInt(pos[0]));
				if (attribute.getStart() < 1)
					throw new IllegalArgumentException(
							"invalid start position attribute \""+name+"\" value "
									+ "for entity "+getCanonicalName());
				if (pos[1].contains(".")) {
					if (attribute.getType() != DataType.FLOAT && attribute.getType() != DataType.DOUBLE)
						throw new IllegalArgumentException("invalid decimal fraction from attribute \""+name+"\""
								+ ", the type should by float or double");
					String p[] = pos[1].split("\\.", 2);
					attribute.setEnd(Integer.parseInt(p[0]));
					attribute.setDecimal(Integer.parseInt(p[1]));
					if (attribute.getDecimal() < 1)
						throw new IllegalArgumentException(
								"invalid decimal position attribute \""+name+"\" value "
										+ "for entity "+getCanonicalName());
				} else {
					attribute.setEnd(Integer.parseInt(pos[1]));
				}
				if (attribute.getEnd() < 1)
					throw new IllegalArgumentException(
							"invalid end position attribute \""+name+"\" value "
									+ "for entity "+getCanonicalName());
			} else {
				tokens.addFirst(value);
			}
		}
		
		while (!tokens.isEmpty()) {
			String value = tokens.pop();
			if (value.equals("required")) {
				attribute.setRequired(true);
			} else if (value.startsWith("required=")) {
				String p[] = value.split("=", 2);
				attribute.setRequired(Boolean.parseBoolean(p[1]));
			} else if (value.startsWith("indexable=")) {
				String p[] = value.split("=", 2);
				attribute.setIndexable(Boolean.parseBoolean(p[1]));
			} else {
				throw new IllegalArgumentException(
						"unknow option \""+value+"\" from attribute \""+name+"\" "
								+ "for entity "+getCanonicalName());
			}
		}
		return this;
	}
	public Attribute addAttribute(String name, DataType type) {
		if (name == null || "".equals(name))
			throw new IllegalArgumentException("the attribute cannot be empty for entity "+getCanonicalName());
		if (hasAttribute(name))
			throw new IllegalArgumentException("the attribute "+name+" "+type.name().toLowerCase()
					+" is already defined for entity "+getCanonicalName());
		if (!name.matches("[_a-zA-Z][_a-zA-Z0-9]*"))
			throw new IllegalArgumentException("the attribute "+name+" "+type.name().toLowerCase()
					+" has invalid name for entity "+getCanonicalName());
		Attribute attribute = new Attribute(this, name, type);
		attributes.add(attribute);
		rebuildAttributeIndexes();
		return attribute;
	}
	public boolean removeAttribute(String name) {
		Attribute a;
		if ((a = getAttribute(name)) != null) {
			attributes.remove(a);
			rebuildAttributeIndexes();
			return true;
		} else {
			return false;
		}
	}
	
	private void rebuildAttributeIndexes() {
		for (int i=0;i<attributes.size();i++)
			attributesIndexes.put(attributes.get(i).getName(), i);
	}
	
	public String getDirectory() {
		return directory;
	}
	public EntityDescriptor setDirectory(String directory) {
		this.directory = directory;
		return this;
	}
	public int getShards() {
		return shards;
	}
	public EntityDescriptor setShards(int shares) {
		this.shards = shares;
		return this;
	}
	public boolean useDynamics() {
		return dynamics;
	}
	public EntityDescriptor setDynamics(boolean useDynamics) {
		this.dynamics = useDynamics;
		return this;
	}
}
