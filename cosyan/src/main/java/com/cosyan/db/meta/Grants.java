package com.cosyan.db.meta;

import java.util.Collection;

import com.cosyan.db.auth.AuthToken;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Grants {

  public static enum Method {
    SELECT, INSERT, DELETE, UPDATE, ALL
  }

  public static class GrantToken {
    private final String username;
    private final Method method;
    private final String tablename;
    private final boolean withGrantOption;

    public GrantToken(String username, Method method, String tablename, boolean withGrantOption) {
      this.username = username;
      this.method = method;
      this.tablename = tablename;
      this.withGrantOption = withGrantOption;
    }

    public boolean includes(GrantToken other) {
      return username.equals(other.username) &&
          tablename.equals(other.tablename) &&
          (withGrantOption || !other.withGrantOption) &&
          (method == Method.ALL || method == other.method);
    }

    public boolean hasGrant(AuthToken authToken) {
      return authToken.isAdmin() || (withGrantOption && username.equals(authToken.username()));
    }
  }

  private final Multimap<String, GrantToken> tableGrants;

  public Grants() {
    tableGrants = HashMultimap.create();
  }

  public void simpleGrant(String username, Method method, String tablename) {
    if (hasAuthorization(username, method, tablename)) {
      return;
    }
    tableGrants.put(tablename, new GrantToken(username, method, tablename, false));
  }

  public void checkAuthorization(String username, Method method, String tablename) throws GrantException {
    if (!hasAuthorization(username, method, tablename)) {
      throw new GrantException(String.format("User '%s' has no authorization to perform '%s' on '%s'.",
          username, method, tablename));
    }
  }

  public boolean hasAuthorization(String username, Method method, String tablename) {
    GrantToken newGrant = new GrantToken(username, method, tablename, false);
    Collection<GrantToken> grants = tableGrants.get(tablename);
    for (GrantToken grant : grants) {
      if (grant.includes(newGrant)) {
        return true;
      }
    }
    return false;
  }

  public void grant(GrantToken grantToken, AuthToken authToken) throws GrantException {
    String table = grantToken.tablename;
    Collection<GrantToken> grants = tableGrants.get(table);
    if (authToken.isAdmin()) {
      tableGrants.put(table, grantToken);
      return;
    }
    for (GrantToken grant : grants) {
      if (grant.hasGrant(authToken)) {
        tableGrants.put(table, grantToken);
        return;
      }
    }
    throw new GrantException(String.format("User '%s' has no grant right on '%s'.", authToken.username(), table));
  }

  public static class GrantException extends Exception {

    private static final long serialVersionUID = 1L;

    public GrantException(String msg) {
      super(msg);
    }
  }
}
