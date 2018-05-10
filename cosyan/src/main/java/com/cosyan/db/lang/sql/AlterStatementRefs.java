package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.TableDefinition.RefDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.References.RefTableMeta;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementRefs {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddRef extends Node implements AlterStatement {
    private final Ident table;
    private final RefDefinition ref;

    private RefTableMeta refTableMeta;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException, GrantException {
      MaterializedTable tableMeta = metaRepo.table(table);
      refTableMeta = tableMeta.createRef(ref);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(table.getString());
      tableMeta.addRef(new TableRef(ref.getName().getString(), ref.getSelect().print(), refTableMeta));
      return Result.META_OK;
    }
  }
}
