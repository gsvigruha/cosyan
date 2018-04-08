package com.cosyan.db.meta;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Grants {

  public static enum Method {
    SELECT, INSERT, DELETE, UPDATE, ALL
  }

  public static class Grant {
    private final String username;
    private final Method method;
    private final String tablename;
    private final boolean withGrantOption;

    public Grant(String username, Method method, String tablename, boolean withGrantOption) {
      this.username = username;
      this.method = method;
      this.tablename = tablename;
      this.withGrantOption = withGrantOption;
    }

    public boolean includes(Grant other) {
      return username.equals(other.username) &&
          tablename.equals(other.tablename) &&
          (withGrantOption || !other.withGrantOption) &&
          (method == Method.ALL || method == other.method);
    }
  }

  private final Multimap<String, Grant> tableGrants;

  public Grants() {
    tableGrants = HashMultimap.create();
  }

  public void simpleGrant(String username, Method method, String tablename) {
    if (hasAuthorization(username, method, tablename)) {
      return;
    }
    tableGrants.put(tablename, new Grant(username, method, tablename, false));
  }

  public void checkAuthorization(String username, Method method, String tablename) throws GrantException {
    if (!hasAuthorization(username, method, tablename)) {
      throw new GrantException(String.format("User '%s' has no authorization to perform '%s' on '%s'.",
          username, method, tablename));
    }
  }

  public boolean hasAuthorization(String username, Method method, String tablename) {
    Grant newGrant = new Grant(username, method, tablename, false);
    Collection<Grant> grants = tableGrants.get(tablename);
    for (Grant grant : grants) {
      if (grant.includes(newGrant)) {
        return true;
      }
    }
    return false;
  }

  public static class GrantException extends Exception {

    private static final long serialVersionUID = 1L;

    public GrantException(String msg) {
      super(msg);
    }
  }
}
