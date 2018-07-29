package com.cosyan.db.entity;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.Statements.GlobalStatement;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;

public class EntityMetaStatement extends GlobalStatement {

  @Override
  public EntityMeta execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
    return new EntityMeta(metaRepo, authToken);
  }
}
