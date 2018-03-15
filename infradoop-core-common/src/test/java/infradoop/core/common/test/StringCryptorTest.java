package infradoop.core.common.test;

import org.junit.Assert;
import org.junit.Test;

import infradoop.core.common.StringCryptor;

public class StringCryptorTest {
	@Test
	public void test() {
		String enc = StringCryptor.encrypt("Jared González");
		Assert.assertEquals(StringCryptor.decrypt(enc), "Jared González");
	}
}
