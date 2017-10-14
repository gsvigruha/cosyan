package com.cosyan.db.sql;

import java.io.IOException;

import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.Result.MetaStatementResult;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.MetaStatement;
import com.cosyan.db.sql.SyntaxTree.Node;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DropStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Drop extends Node implements MetaStatement {
    private final Ident table;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      if (!tableMeta.reverseForeignKeys().isEmpty()) {
        ReverseForeignKey foreignKey = tableMeta.reverseForeignKeys().values().iterator().next();
        throw new ModelException(String.format("Cannot drop table '%s', referenced by foreign key '%s.%s'.",
            table.getString(),
            foreignKey.getRefTable().tableName(),
            foreignKey));
      }
      metaRepo.dropTable(table.getString());
      return new MetaStatementResult();
    }
  }
}
