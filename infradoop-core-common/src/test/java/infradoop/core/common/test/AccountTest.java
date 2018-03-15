package infradoop.core.common.test;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import infradoop.core.common.account.Account;
import infradoop.core.common.account.AccountManager;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AccountTest {
	@Test
	public void test_01_simple_account() throws IOException {
		Account account = AccountManager.register(Account.INHERIT);
		Assert.assertTrue(account.getUserGroupInformation().hasKerberosCredentials());
	}
	
	@Test
	public void test_02_other_account() throws IOException {
		Assume.assumeNotNull(System.getProperty("account.1.principal"));
		Assume.assumeNotNull(System.getProperty("account.1.password"));
		Account account = AccountManager.register(Account.DEFAULT,
				System.getProperty("account.1.principal"),
				System.getProperty("account.1.password"));
		Assert.assertTrue(account.getUserGroupInformation().hasKerberosCredentials());
	}
}
