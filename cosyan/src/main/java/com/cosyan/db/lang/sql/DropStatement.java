package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DropStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DropTable extends GlobalStatement {
    private final Ident table;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException, GrantException {
      MaterializedTable tableMeta = metaRepo.table(table);
      if (!tableMeta.foreignKeys().isEmpty()) {
        ForeignKey foreignKey = tableMeta.foreignKeys().values().iterator().next();
        throw new ModelException(String.format("Cannot drop table '%s', has foreign key '%s'.",
            table.getString(), foreignKey),
            table);
      }
      if (!tableMeta.reverseForeignKeys().isEmpty()) {
        ReverseForeignKey foreignKey = tableMeta.reverseForeignKeys().values().iterator().next();
        throw new ModelException(String.format("Cannot drop table '%s', referenced by foreign key '%s.%s'.",
            table.getString(),
            foreignKey.getRefTable().tableName(),
            foreignKey.getReverse()),
            table);
      }
      metaRepo.dropTable(tableMeta, authToken);
      return Result.META_OK;
    }

    @Override
    public boolean log() {
      return true;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DropIndex extends GlobalStatement {
    private final Ident table;
    private final Ident column;

    private BasicColumn basicColumn;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException, GrantException {
      MaterializedTable tableMeta = metaRepo.table(table);
      basicColumn = tableMeta.column(column);
      if (basicColumn.isUnique()) {
        throw new ModelException(String.format("Cannot drop index '%s.%s', column is unique.",
            tableMeta.tableName(), basicColumn.getName()), column);
      }
      metaRepo.dropIndex(tableMeta, basicColumn, authToken);
      return Result.META_OK;
    }

    @Override
    public boolean log() {
      return true;
    }
  }
}
