package com.cosyan.db.auth;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.conf.Config;

public class AuthenticatorTest {

  private static Config config;

  @BeforeClass
  public static void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    config = new Config(props);
  }

  @Test
  public void testAuth() throws AuthException {
    Authenticator auth = new Authenticator(config);
    AuthToken token = auth.auth("admin", "admin", Method.LOCAL);
    assertEquals("admin", token.username());
    assertTrue(token.isAdmin());
  }
}
