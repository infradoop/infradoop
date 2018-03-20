package infradoop.core.common.entity;

import java.io.IOException;

import infradoop.core.common.data.DefinedVariables;
import infradoop.core.common.data.StringDataConverter;
import infradoop.core.common.data.VariableResolver;

public class EntityRowVariableResolver implements VariableResolver {
	private final DefinedVariables definedVariables;
	private EntityRow entityRow;
	
	public EntityRowVariableResolver(DefinedVariables definedVariables) {
		this.definedVariables = definedVariables;
	}
	
	@Override
	public String resolve(String variable, String[] params) {
		int attrIndex;
		if (definedVariables != null && definedVariables.containsKey(variable)) {
			String v = definedVariables.get(variable);
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
