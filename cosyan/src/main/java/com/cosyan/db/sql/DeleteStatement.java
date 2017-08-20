package com.cosyan.db.sql;

import java.io.IOException;

import com.cosyan.db.io.TableWriter.TableDeleter;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DeleteStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Delete extends Node implements Statement {
    private final Ident table;
    private final Expression where;

    @Override
    public boolean execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = (MaterializedTableMeta) metaRepo.table(table);
      DerivedColumn whereColumn = where.compile(tableMeta, metaRepo);
      TableDeleter deleter = tableMeta.deleter(whereColumn);
      deleter.delete();
      deleter.close();
      return true;
    }
  }
}
