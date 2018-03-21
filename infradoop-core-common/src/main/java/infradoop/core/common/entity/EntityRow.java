package infradoop.core.common.entity;

import java.io.IOException;

public interface EntityRow {
	public Attribute getAttribute(int index);
	public Attribute getAttribute(String name);
	public int indexOfAttribute(String name);
	public int countAttributes();
	public boolean hasValue(int index) throws IOException;
	public Object getValue(int index) throws IOException;
	public void setValue(int index, String value) throws IOException;
	public void setValue(int index, Object value) throws IOException;
}
