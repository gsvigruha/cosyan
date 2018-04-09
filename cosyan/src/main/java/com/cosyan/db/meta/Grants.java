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

  public interface GrantToken {
    public String username();

    public Method method();

    public boolean withGrantOption();

    public boolean hasAccess(Method method, MaterializedTableMeta tableMeta, AuthToken authToken);

    public String objects();

    public boolean providesGrantTo(GrantToken grantToken);

    public boolean ownedBy(AuthToken authToken);
  }

  public static class GrantAllTablesToken implements GrantToken {
    private final String username;
    private final Method method;
    private final boolean withGrantOption;

    public GrantAllTablesToken(String username, Method method, boolean withGrantOption) {
      this.username = username;
      this.method = method;
      this.withGrantOption = withGrantOption;
    }

    @Override
    public String username() {
      return username;
    }

    @Override
    public Method method() {
      return method;
    }

    @Override
    public String objects() {
      return "*";
    }

    @Override
    public boolean withGrantOption() {
      return withGrantOption;
    }

    @Override
    public boolean hasAccess(Method method, MaterializedTableMeta tableMeta, AuthToken authToken) {
      return authToken.isAdmin()
          || (authToken.username().equals(username)
              && (method == this.method || this.method == Method.ALL));
    }

    @Override
    public boolean providesGrantTo(GrantToken proposedGrantToken) {
      return this.withGrantOption
          && (this.method == proposedGrantToken.method() || this.method == Method.ALL);
    }

    @Override
    public boolean ownedBy(AuthToken authToken) {
      return authToken.isAdmin();
    }
  }

  public static class GrantTableToken implements GrantToken {
    private final String username;
    private final Method method;
    private final MaterializedTableMeta table;
    private final boolean withGrantOption;

    public GrantTableToken(String username, Method method, MaterializedTableMeta table, boolean withGrantOption) {
      this.username = username;
      this.method = method;
      this.table = table;
      this.withGrantOption = withGrantOption;
    }

    @Override
    public String username() {
      return username;
    }

    @Override
    public Method method() {
      return method;
    }

    @Override
    public String objects() {
      return table.tableName();
    }

    @Override
    public boolean withGrantOption() {
      return withGrantOption;
    }

    @Override
    public boolean hasAccess(Method method, MaterializedTableMeta tableMeta, AuthToken authToken) {
      return authToken.isAdmin()
          || (authToken.username().equals(username)
              && (method == this.method || this.method == Method.ALL)
              && tableMeta == this.table);
    }

    @Override
    public boolean providesGrantTo(GrantToken proposedGrantToken) {
      return this.withGrantOption
          && (this.method == proposedGrantToken.method() || this.method == Method.ALL)
          && this.objects().equals(proposedGrantToken.objects());
    }

    @Override
    public boolean ownedBy(AuthToken authToken) {
      return authToken.isAdmin() || table.owner().equals(authToken.username());
    }
  }

  private final Config config;
  private final Multimap<String, GrantToken> userGrants;

  public Grants(Config config) {
    this.config = config;
    userGrants = HashMultimap.create();
  }

  public void createGrant(GrantToken grantToken, AuthToken authToken) throws GrantException {
    String username = grantToken.username();
    if (grantToken.ownedBy(authToken)) {
      userGrants.put(username, grantToken);
      return;
    }
    Collection<GrantToken> grants = userGrants.get(authToken.username());
    for (GrantToken grant : grants) {
      if (grant.providesGrantTo(grantToken)) {
        userGrants.put(username, grantToken);
        return;
      }
    }
    throw new GrantException(
        String.format("User '%s' has no grant %s right on '%s'.", authToken.username(), grantToken.method(), grantToken.objects()));
  }

  public void createUser(String username, String password, AuthToken authToken) throws GrantException, IOException {
    if (!authToken.isAdmin()) {
      throw new GrantException("Only the administrator can create users.");
    }
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

  private void checkAccess(
      Collection<GrantToken> grants, MaterializedTableMeta tableMeta, Method method, String table, AuthToken authToken)
      throws GrantException {
    for (GrantToken grant : grants) {
      if (grant.hasAccess(method, tableMeta, authToken)) {
        return;
      }
    }
    throw new GrantException(String.format("User '%s' has no %s right on '%s'.", authToken.username(), method, table));
  }

  public void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
    MaterializedTableMeta tableMeta = resource.getTableMeta();
    String table = tableMeta.tableName();
    if (authToken.isAdmin() || authToken.username().equals(resource.getTableMeta().owner())) {
      return;
    }
    Collection<GrantToken> grants = userGrants.get(authToken.username());
    if (resource.write()) {
      if (resource.isInsert()) {
        checkAccess(grants, tableMeta, Method.INSERT, table, authToken);
      }
      if (resource.isDelete()) {
        checkAccess(grants, tableMeta, Method.DELETE, table, authToken);
      }
      if (resource.isUpdate()) {
        checkAccess(grants, tableMeta, Method.UPDATE, table, authToken);
      }
    } else {
      if (resource.isSelect()) {
        checkAccess(grants, tableMeta, Method.SELECT, table, authToken);
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
