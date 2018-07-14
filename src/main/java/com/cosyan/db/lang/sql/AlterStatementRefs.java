package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.lang.expr.TableDefinition.AggRefDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.References.AggRefTableMeta;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementRefs {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddAggRef extends AlterStatement {
    private final Ident table;
    private final AggRefDefinition ref;

    private AggRefTableMeta refTableMeta;

    @Override
    public MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException {
      MaterializedTable tableMeta = metaRepo.table(table);
      refTableMeta = tableMeta.createAggRef(ref);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result executeData(MetaRepoExecutor metaRepo, Resources resources) {
      MaterializedTable tableMeta = resources.meta(table.getString());
      tableMeta.addRef(new TableRef(
    		  ref.getName().getString(), ref.getSelect().print(), metaRepo.maxRefIndex() + 1, refTableMeta));
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropAggRef extends GlobalStatement {
    private final Ident table;
    private final Ident ref;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException, IOException {
      MaterializedTable tableMeta = metaRepo.table(table);
      if (tableMeta.hasAggRef(ref.getString())) {
        tableMeta.dropAggRef(ref);
      } else {
        throw new ModelException(String.format("Aggref '%s' not found in table '%s'.", ref, table), ref);
      }
      return Result.META_OK;
    }

    @Override
    public boolean log() {
      return true;
    }
  }
}
