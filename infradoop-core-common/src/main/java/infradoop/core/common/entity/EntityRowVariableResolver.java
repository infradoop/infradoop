package infradoop.core.common.entity;

import java.io.IOException;
import java.util.Map;

import infradoop.core.common.data.StringDataConverter;
import infradoop.core.common.data.VariableResolver;

public class EntityRowVariableResolver implements VariableResolver {
	private final Map<String, String> map;
	private EntityRow entityRow;
	
	public EntityRowVariableResolver(Map<String, String> map) {
		this.map = map;
	}
	
	@Override
	public String resolve(String variable, String[] params) {
		int attrIndex;
		if (map != null && map.containsKey(variable)) {
			String v = map.get(variable);
			if (v == null)
				return "";
			else
				return v;
		} else if (getCurrent() != null && (attrIndex = getCurrent().indexOfAttribute(variable)) >= 0) {
			try {
				return StringDataConverter.toString(getCurrent().getValue(attrIndex));
			} catch (IOException e) {
				return "";
			}
		}
		return "";
	}

	@Override
	public EntityRow getCurrent() {
		return entityRow;
	}
	@Override
	public void setCurrent(Object current) {
		entityRow = (EntityRow)current;
	}
}
