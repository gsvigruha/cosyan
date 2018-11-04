/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.meta;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.LocalUsers;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class Grants {

  public static enum Method {
    SELECT, INSERT, DELETE, UPDATE, ALL
  }

  public static class GrantToken {
    private final String username;
    private final Method method;
    private final String owner;
    private final String table;
    private final boolean withGrantOption;

    public GrantToken(String username, Method method, String owner, String table, boolean withGrantOption) {
      this.username = username;
      this.method = method;
      this.owner = owner;
      this.table = table;
      this.withGrantOption = withGrantOption;
    }

    public String username() {
      return username;
    }

    public Method method() {
      return method;
    }

    public String print() {
      return owner + "." + table;
    }

    public boolean withGrantOption() {
      return withGrantOption;
    }

    private boolean tableMatches(DBObject object) {
      return (owner.equals("*") || owner.equals(object.owner())) && (table.equals("*") || table.equals(object.name()));
    }

    public boolean hasAccess(Method method, DBObject object, AuthToken authToken) {
      return authToken.isAdmin()
          || (authToken.username().equals(username)
              && (method == this.method || this.method == Method.ALL)
              && tableMatches(object));
    }

    public boolean providesGrantTo(GrantToken proposedGrantToken) {
      return this.withGrantOption
          && (this.method == proposedGrantToken.method() || this.method == Method.ALL)
          && (this.table.equals("*") || this.table.equals(proposedGrantToken.table));
    }

    public boolean ownedBy(AuthToken authToken) {
      return authToken.isAdmin() || owner.equals(authToken.username());
    }

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("method", method.name());
      obj.put("grant", withGrantOption);
      obj.put("table_owner", owner);
      obj.put("table_name", table);
      return obj;
    }
  }

  private final LocalUsers localUsers;
  private final Multimap<String, GrantToken> userGrants;

  public Grants(LocalUsers localUsers) {
    this.userGrants = HashMultimap.create();
    this.localUsers = localUsers;
  }

  public void fromJSON(JSONArray tokensObj, Map<String, Map<String, MaterializedTable>> tables) {
    Multimap<String, GrantToken> tokens = HashMultimap.create();
    Map<String, String> users = new HashMap<>();
    for (int i = 0; i < tokensObj.length(); i++) {
      String username = tokensObj.getJSONObject(i).getString("username");
      if (tokensObj.getJSONObject(i).has("password")) {
        users.put(username, tokensObj.getJSONObject(i).getString("password"));
      }
      if (tokensObj.getJSONObject(i).has("tokens")) {
        JSONArray arr = tokensObj.getJSONObject(i).getJSONArray("tokens");
        for (int j = 0; j < arr.length(); j++) {
          JSONObject tokenObj = arr.getJSONObject(j);
          tokens.put(username, new GrantToken(
              username,
              Method.valueOf(tokenObj.getString("method")),
              tokenObj.getString("table_owner"),
              tokenObj.getString("table_name"),
              tokenObj.getBoolean("grant")));
        }
      }
    }
    userGrants.clear();
    userGrants.putAll(tokens);
    localUsers.reload(users);
  }

  public JSONArray toJSON() {
    JSONArray arr = new JSONArray();
    Collection<String> users = Sets.union(localUsers.users(), userGrants.keySet());
    for (String username : users) {
      JSONObject userObj = new JSONObject();
      userObj.put("username", username);
      if (localUsers.isLocalUser(username)) {
        userObj.put("password", localUsers.hashedPW(username));
      }
      if (userGrants.containsKey(username)) {
        JSONArray tokenObj = new JSONArray();
        for (GrantToken token : userGrants.get(username)) {
          tokenObj.put(token.toJSON());
        }
        userObj.put("tokens", tokenObj);
      }
      arr.put(userObj);
    }
    return arr;
  }

  public void createGrant(GrantToken proposedGrantToken, AuthToken authToken) throws GrantException {
    String username = proposedGrantToken.username();
    if (proposedGrantToken.ownedBy(authToken)) {
      userGrants.put(username, proposedGrantToken);
      return;
    }
    // Check if the current user have an existing grant allowing to create this new grant.
    Collection<GrantToken> existingGrants = userGrants.get(authToken.username());
    for (GrantToken existingGrant : existingGrants) {
      if (existingGrant.providesGrantTo(proposedGrantToken)) {
        userGrants.put(username, proposedGrantToken);
        return;
      }
    }
    throw new GrantException(
        String.format("User '%s' has no grant %s right on '%s'.", authToken.username(), proposedGrantToken.method(),
            proposedGrantToken.print()));
  }

  public void createUser(String username, String password, AuthToken authToken) throws GrantException {
    if (!authToken.isAdmin()) {
      throw new GrantException("Only the administrator can create users.");
    }
    try {
      localUsers.createUser(username, password);
    } catch (AuthException e) {
      throw new GrantException(e);
    }
  }

  private void checkAccess(
      Collection<GrantToken> grants, DBObject object, Method method, AuthToken authToken)
      throws GrantException {
    if (!hasAccess(grants, object, method, authToken)) {
      throw new GrantException(
          String.format("User '%s' has no %s right on '%s'.", authToken.username(), method, object.fullName()));
    }
  }

  private boolean hasAccess(
      Collection<GrantToken> grants, DBObject object, Method method, AuthToken authToken) {
    for (GrantToken grant : grants) {
      if (grant.hasAccess(method, object, authToken)) {
        return true;
      }
    }
    return false;
  }

  public void checkAccess(TableMetaResource resource, AuthToken authToken) throws GrantException {
    DBObject object = resource.getObject();
    if (authToken.isAdmin() || authToken.username().equals(resource.getObject().owner())) {
      return;
    }
    Collection<GrantToken> grants = userGrants.get(authToken.username());
    if (resource.write()) {
      if (resource.isInsert()) {
        checkAccess(grants, object, Method.INSERT, authToken);
      }
      if (resource.isDelete()) {
        checkAccess(grants, object, Method.DELETE, authToken);
      }
      if (resource.isUpdate()) {
        checkAccess(grants, object, Method.UPDATE, authToken);
      }
    } else {
      if (resource.isSelect()) {
        checkAccess(grants, object, Method.SELECT, authToken);
      }
    }
  }

  public boolean hasAccess(MaterializedTable tableMeta, AuthToken authToken) {
    if (authToken.isAdmin() || authToken.username().equals(tableMeta.owner())) {
      return true;
    }
    Collection<GrantToken> grants = userGrants.get(authToken.username());
    return hasAccess(grants, tableMeta, Method.SELECT, authToken);
  }

  public boolean hasAccess(MaterializedTable tableMeta, AuthToken authToken, Method method) {
    if (authToken.isAdmin() || authToken.username().equals(tableMeta.owner())) {
      return true;
    }
    Collection<GrantToken> grants = userGrants.get(authToken.username());
    return hasAccess(grants, tableMeta, method, authToken);
  }

  public void checkOwner(DBObject object, AuthToken authToken) throws GrantException {
    if (authToken.isAdmin() || authToken.username().equals(object.owner())) {
      return;
    }
    throw new GrantException(String.format("User '%s' has no ownership right on '%s'.",
        authToken.username(), object.fullName()));
  }

  public static class GrantException extends Exception {

    private static final long serialVersionUID = 1L;

    public GrantException(String msg) {
      super(msg);
    }

    public GrantException(Exception cause) {
      super(cause);
    }
  }
}
