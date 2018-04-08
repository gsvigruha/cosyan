package com.cosyan.db.meta;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.session.Session;

public class GrantsTest extends UnitTestBase {

  @Test
  public void testAdminGrant() throws AuthException {
    execute("create table t1 (a varchar);");
    execute("grant select on t1 to admin;");
  }

  @Test
  public void testNewUser() throws AuthException {
    execute("create table t2 (a varchar);");
    execute("create user u1 identified by 'abc';");
    Session u1 = dbApi.authSession("u1", "abc", Method.LOCAL);

  }
}
