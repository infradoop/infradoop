package infradoop.core.common.data;

public interface VariableResolver {
	public String resolve(String variable, String params[]);
	
	public Object getCurrent();
	public void setCurrent(Object current);
}
