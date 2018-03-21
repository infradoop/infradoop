package infradoop.core.common.data;

public interface DefinedVariables {
	public String get(String key);
	public boolean containsKey(String key);
	public void set(String key, String value);
}
