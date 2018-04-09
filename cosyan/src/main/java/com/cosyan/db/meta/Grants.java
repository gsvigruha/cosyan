package com.cosyan.db.meta;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.LocalUsers;
import com.cosyan.db.conf.Config;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Grants {

  public static enum Method {
    SELECT, INSERT, DELETE, UPDATE, ALL
  }

  public static class GrantToken {
    private final String username;
    private final Method method;
    private final MaterializedTableMeta table;
    private final boolean withGrantOption;

    public GrantToken(String username, Method method, MaterializedTableMeta table, boolean withGrantOption) {
      this.username = username;
      this.method = method;
      this.table = table;
      this.withGrantOption = withGrantOption;
    }

    public boolean includes(GrantToken other) {
      return username.equals(other.username) &&
          this.table == other.table &&
          (this.withGrantOption || !other.withGrantOption) &&
          (this.method == Method.ALL || this.method == other.method);
    }

    public boolean hasGrant(AuthToken authToken) {
      return authToken.isAdmin() || (withGrantOption && username.equals(authToken.username()));
    }

    public boolean hasAccess(Method method, AuthToken authToken) {
      return hasGrant(authToken)
          || (authToken.username().equals(username)
              && (method == this.method || this.method == Method.ALL));
    }
  }

  private final Config config;
  private final Multimap<String, GrantToken> tableGrants;

  public Grants(Config config) {
    this.config = config;
    tableGrants = HashMultimap.create();
  }

  public void grant(GrantToken grantToken, AuthToken authToken) throws GrantException {
    String table = grantToken.table.tableName();
    if (authToken.isAdmin() || authToken.username().equals(grantToken.table.owner())) {
      tableGrants.put(table, grantToken);
      return;
    }
    Collection<GrantToken> grants = tableGrants.get(table);
    for (GrantToken grant : grants) {
      if (grant.hasGrant(authToken)) {
        tableGrants.put(table, grantToken);
        return;
      }
    }
    throw new GrantException(String.format("User '%s' has no grant right on '%s'.", authToken.username(), table));
  }

  public void createUser(String username, String password, AuthToken authToken) throws GrantException, IOException {
    BufferedReader reader = new BufferedReader(new FileReader(config.usersFile()));
    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(":");
        if (username.equals(parts[0])) {
          throw new GrantException(String.format("User '%s' already exists.", username));
        }
      }
    } finally {
      reader.close();
    }
    BufferedWriter writer = new BufferedWriter(new FileWriter(config.usersFile(), true));
    try {
      writer.write("\n" + username + ":" + LocalUsers.hash(password));
    } catch (NoSuchAlgorithmException e) {
      throw new GrantException(e.getMessage());
    } finally {
      writer.close();
    }
  }

  private void checkAccess(Collection<GrantToken> grants, Method method, String table, AuthToken authToken)
      throws GrantException {
    for (GrantToken grant : grants) {
      if (grant.hasAccess(method, authToken)) {
        return;
      }
    }
    throw new GrantException(String.format("User '%s' has no %s right on '%s'.", authToken.username(), method, table));
  }

  public void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
    String table = resource.getTableMeta().tableName();
    if (authToken.isAdmin() || authToken.username().equals(resource.getTableMeta().owner())) {
      return;
    }
    Collection<GrantToken> grants = tableGrants.get(table);
    if (resource.write()) {

      if (resource.isInsert()) {
        checkAccess(grants, Method.INSERT, table, authToken);
      }
      if (resource.isDelete()) {
        checkAccess(grants, Method.DELETE, table, authToken);
      }
      if (resource.isUpdate()) {
        checkAccess(grants, Method.UPDATE, table, authToken);
      }
    } else {
      if (resource.isSelect()) {
        checkAccess(grants, Method.SELECT, table, authToken);
      }
    }
  }

  public static class GrantException extends Exception {

    private static final long serialVersionUID = 1L;

    public GrantException(String msg) {
      super(msg);
    }
  }
}
