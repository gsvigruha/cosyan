package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.lang.sql.CreateStatement.RefDefinition;
import com.cosyan.db.lang.sql.Result.MetaStatementResult;
import com.cosyan.db.lang.sql.SelectStatement.Select.TableColumns;
import com.cosyan.db.lang.sql.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.DerivedTables.ReferencedDerivedTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.References.ReferencedAggrTableMeta;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableRef;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementRefs {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddRef extends Node implements MetaStatement {
    private final Ident table;
    private final RefDefinition ref;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      if (!tableMeta.isEmpty()) {
        throw new ModelException(String.format("Cannot add ref to a non-empty table."));
      }
      ReferencedMultiTableMeta srcTableMeta = (ReferencedMultiTableMeta) ref.getSelect().getTable().compile(tableMeta.reader());
      ReferencedAggrTableMeta aggrTable = new ReferencedAggrTableMeta(
          new KeyValueTableMeta(
              srcTableMeta,
              TableMeta.wholeTableKeys), srcTableMeta.getReverseForeignKey());
      // Columns have aggregations, recompile with an AggrTable.
      TableColumns tableColumns = SelectStatement.Select.tableColumns(aggrTable, ref.getSelect().getColumns());
      ReferencedDerivedTableMeta refTableMeta = new ReferencedDerivedTableMeta(aggrTable, tableColumns.getColumns());
      tableMeta.addRef(new TableRef(ref.getName(), ref.getSelect(), refTableMeta));
      return new MetaStatementResult();
    }
  }
}
