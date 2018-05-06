package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Users {
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateUser extends Node implements AlterStatement {

    private final Ident username;
    private final StringLiteral password;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, GrantException {
      try {
        metaRepo.createUser(username.getString(), password.getValue(), authToken);
      } catch (IOException e) {
        throw new GrantException(e);
      }
      return MetaResources.empty();
    }

    @Override
    public boolean log() {
      // Users are stored in a separate file manually. This statement should not be
      // logged in order to avoid storing un-hashed passwords and re-creating users on
      // DB restart.
      return false;
    }

    @Override
    public Result execute(MetaRepo metaRepo, Resources resources) throws RuleException, IOException {
      return Result.META_OK;
    }
  }
}