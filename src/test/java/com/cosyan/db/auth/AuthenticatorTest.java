package com.cosyan.db.auth;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;

public class AuthenticatorTest extends UnitTestBase {

  @Test
  public void testAuth() throws Exception {
    Authenticator auth = dbApi.authenticator();
    AuthToken token = auth.auth("admin", "admin", Method.LOCAL);
    assertEquals("admin", token.username());
    assertTrue(token.isAdmin());
  }

  @Test
  public void testCreateUser() throws Exception {
    Authenticator auth = dbApi.authenticator();
    auth.localUsers().createUser("u1", "abc");
    AuthToken token = auth.auth("u1", "abc", Method.LOCAL);
    assertEquals("u1", token.username());
    assertFalse(token.isAdmin());
  }

  @Test
  public void testInvalidLogin() throws Exception {
    Authenticator auth = dbApi.authenticator();
    try {
      auth.auth("u2", "abc", Method.LOCAL);
      fail();
    } catch (AuthException e) {
      assertEquals("Wrong username/password.", e.getMessage());
    }
    auth.localUsers().createUser("u2", "abc");
    try {
      auth.auth("u2", "abcd", Method.LOCAL);
      fail();
    } catch (AuthException e) {
      assertEquals("Wrong username/password.", e.getMessage());
    }
    auth.auth("u2", "abc", Method.LOCAL);
  }
}
