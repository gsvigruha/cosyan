package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.sql.CreateStatement.ConstraintDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.MetaStatementResult;
import com.cosyan.db.meta.MaterializedTableMeta;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementConstraints {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddConstraint extends Node implements MetaStatement {
    private final Ident table;
    private final ConstraintDefinition constraint;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      CreateStatement.CreateTable.addConstraints(metaRepo, tableMeta, ImmutableList.of(constraint));
      metaRepo.sync(tableMeta);
      return new MetaStatementResult();
    }

    @Override
    public boolean log() {
      return true;
    }
  }
}
