package com.cosyan.db.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;

public class AuthenticatorTest extends UnitTestBase {

  @Test
  public void testAuth() throws AuthException {
    Authenticator auth = new Authenticator(config);
    AuthToken token = auth.auth("admin", "admin", Method.LOCAL);
    assertEquals("admin", token.username());
    assertTrue(token.isAdmin());
  }
}
