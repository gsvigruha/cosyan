package com.cosyan.db.lang.sql;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Users {
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CreateUser extends GlobalStatement {

    private final Ident username;
    private final StringLiteral password;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException {
      metaRepo.createUser(username.getString(), password.getValue(), authToken);
      return Result.META_OK;
    }
  }
}