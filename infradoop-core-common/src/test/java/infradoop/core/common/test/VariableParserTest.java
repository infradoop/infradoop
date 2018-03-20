package infradoop.core.common.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import infradoop.core.common.data.DefaultVariableResolver;
import infradoop.core.common.data.VariableParser;
import infradoop.core.common.data.VariableResolver;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VariableParserTest {
	@Test
	public void test_01_simple() {
		Map<String, String> map = new HashMap<>();
		map.put("var1", "value1");
		map.put("var2", "value2");
		VariableResolver varResolver = new DefaultVariableResolver(map);
		Assert.assertEquals(VariableParser.parse("test${var1},${var2}", varResolver), "testvalue1,value2");
	}
	
	@Test
	public void test_02_bulk() {
		Map<String, String> map = new HashMap<>();
		map.put("var1", "value1");
		map.put("var2", "value2");
		VariableResolver varResolver = new DefaultVariableResolver(map);
		for (int i=0;i<1000000;i++) {
			VariableParser.parse("test${var1},${var2}", varResolver);
		}
	}
}
