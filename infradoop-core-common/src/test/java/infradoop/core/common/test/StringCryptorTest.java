package infradoop.core.common.test;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import infradoop.core.common.StringCryptor;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StringCryptorTest {
	@Test
	public void test_01_encrypt() {
		String enc = StringCryptor.encrypt("Jared González");
		Assert.assertEquals(StringCryptor.decrypt(enc), "Jared González");
	}
	
	@Test
	public void test_01_decript() {
		Assume.assumeNotNull(System.getProperty("stringEncrypted"));
		System.out.println(StringCryptor.decrypt(System.getProperty("stringEncrypted")));
	}
}
