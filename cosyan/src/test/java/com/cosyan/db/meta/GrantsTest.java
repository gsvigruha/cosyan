package com.cosyan.db.meta;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
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
    ErrorResult e = (ErrorResult) u1.execute("insert into t2 values ('x');");
    assertEquals("User 'u1' has no INSERT right on 't2'.", e.getError().getMessage());
    execute("grant insert on t2 to u1;");
    statement("insert into t2 values ('x');", u1);
  }
}
