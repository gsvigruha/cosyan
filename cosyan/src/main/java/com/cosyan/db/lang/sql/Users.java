package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.MetaStatementResult;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Users {
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateUser extends Node implements MetaStatement {

    private final Ident username;
    private final StringLiteral password;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, IndexException, IOException, GrantException {
      metaRepo.createUser(username.getString(), password.getValue(), authToken);
      return new MetaStatementResult();
    }

    @Override
    public boolean log() {
      // Users are stored in a separate file manually. This statement should not be
      // logged in order to avoid storing un-hashed passwords and re-creating users on
      // DB restart.
      return false;
    }
  }
}