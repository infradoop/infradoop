package infradoop.core.common.data;

import java.util.Map;

public class DefaultVariableResolver implements VariableResolver {
	private final Map<String, String> map;
	
	public DefaultVariableResolver(Map<String, String> map) {
		this.map = map;
	}
	
	@Override
	public String resolve(String variable, String[] params) {		
		String v = map.get(variable);
		if (v == null)
			return "";
		else
			return v;
	}

	@Override
	public Object getCurrent() {
		return null;
	}

	@Override
	public void setCurrent(Object current) {
	}
}
