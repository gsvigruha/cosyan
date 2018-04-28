package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.MetaStatementResult;
import com.cosyan.db.meta.Grants;
import com.cosyan.db.meta.Grants.GrantAllTablesToken;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.Grants.GrantTableToken;
import com.cosyan.db.meta.Grants.GrantToken;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class GrantStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Grant extends Node implements MetaStatement {
    private final Ident user;
    private final Ident table;
    private final String method;
    private final boolean withGrantOption;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException, GrantException {
      GrantToken grantToken;
      if (table.is(Tokens.ASTERISK)) {
        grantToken = new GrantAllTablesToken(
            user.getString(),
            Grants.Method.valueOf(method.toUpperCase()),
            withGrantOption);
      } else {
        grantToken = new GrantTableToken(
            user.getString(),
            Grants.Method.valueOf(method.toUpperCase()),
            metaRepo.table(table),
            withGrantOption);
      }
      metaRepo.createGrant(grantToken, authToken);
      return new MetaStatementResult();
    }

    @Override
    public boolean log() {
      return true;
    }
  }
}
