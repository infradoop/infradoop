package infradoop.core.common.test;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import infradoop.core.common.entity.DataType;
import infradoop.core.common.entity.EntityDescriptor;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EntityDescriptorTest {
	@Test
	public void test_01_validate_attribute_parser() {
		EntityDescriptor entityDesc = new EntityDescriptor("default", "test");
		
		entityDesc.compileAttribute("id bigint required=true");
		Assert.assertEquals(entityDesc.getAttribute("id").getType(), DataType.BIGINT);
		Assert.assertTrue(entityDesc.getAttribute("id").isRequired());
		Assert.assertTrue(entityDesc.getAttribute("id").isIndexable());
		
		entityDesc.compileAttribute("event timestamp \"yyyy-MM-dd\" required=true");
		Assert.assertEquals(entityDesc.getAttribute("event").getType(), DataType.TIMESTAMP);
		Assert.assertTrue(entityDesc.getAttribute("event").isRequired());
		Assert.assertEquals((Object)entityDesc.getAttribute("event").getDateFormat().toPattern(),
				(Object)"yyyy-MM-dd");
		
		entityDesc.compileAttribute("event_oth timestamp \"yyyy-MM-dd\" 5:15 required=true");
		Assert.assertEquals(entityDesc.getAttribute("event_oth").getType(), DataType.TIMESTAMP);
		Assert.assertEquals((Object)entityDesc.getAttribute("event_oth").getDateFormat().toPattern(),
				(Object)"yyyy-MM-dd");
		Assert.assertTrue(entityDesc.getAttribute("event_oth").getStart() == 5);
		Assert.assertTrue(entityDesc.getAttribute("event_oth").getEnd() == 15);
		Assert.assertTrue(entityDesc.getAttribute("event_oth").isRequired());
		
		entityDesc.compileAttribute("category string set \"default\"");
		Assert.assertEquals(entityDesc.getAttribute("category").getType(), DataType.STRING);
		Assert.assertEquals((Object)entityDesc.getAttribute("category").getDynamicValue(), (Object)"default");
		Assert.assertFalse(entityDesc.getAttribute("category").isRequired());
		
		entityDesc.compileAttribute("desc string required=false indexable=false");
		Assert.assertEquals(entityDesc.getAttribute("desc").getType(), DataType.STRING);
		Assert.assertFalse(entityDesc.getAttribute("desc").isRequired());
		Assert.assertFalse(entityDesc.getAttribute("desc").isIndexable());
	}
}
