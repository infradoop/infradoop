package infradoop.core.common.data;

import java.util.Map;

public class DefaultVariableResolver implements VariableResolver {
	private final DefinedVariables definedVariables;
	
	public DefaultVariableResolver(Map<String, String> map) {
		this(new DefaultDefinedVariables(map));
	}
	public DefaultVariableResolver(DefinedVariables definedVariables) {
		this.definedVariables = definedVariables;
	}
	
	@Override
	public String resolve(String variable, String[] params) {		
		String v = definedVariables.get(variable);
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
