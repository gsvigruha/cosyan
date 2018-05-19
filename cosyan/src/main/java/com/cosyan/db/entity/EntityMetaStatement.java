package com.cosyan.db.entity;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.google.common.collect.ImmutableMap;

public class EntityMetaStatement extends GlobalStatement {

  @Override
  public EntityMeta execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
    ImmutableMap<String, MaterializedTable> tables = metaRepo.getTables(authToken);
    return new EntityMeta(tables.values().asList());
  }

  @Override
  public boolean log() {
    return false;
  }
}
