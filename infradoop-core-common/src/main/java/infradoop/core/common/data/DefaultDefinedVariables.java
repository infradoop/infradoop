package infradoop.core.common.data;

import java.util.Map;

public class DefaultDefinedVariables implements DefinedVariables {
	private final Map<String, String> map;
	
	public DefaultDefinedVariables(Map<String, String> map) {
		this.map = map;
	}
	@Override
	public String get(String key) {
		return map.get(key);
	}

	@Override
	public boolean containsKey(String key) {
		return map.containsKey(key);
	}

}
