package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.lang.sql.CreateStatement.RefDefinition;
import com.cosyan.db.lang.sql.Result.MetaStatementResult;
import com.cosyan.db.lang.sql.SelectStatement.Select.TableColumns;
import com.cosyan.db.lang.sql.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables.GlobalAggrTableMeta;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencingDerivedTableMeta;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
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
      ReferencedMultiTableMeta srcTableMeta = (ReferencedMultiTableMeta) ref.getSelect().getTable()
          .compile(tableMeta.reader());
      ExposedTableMeta derivedTable;
      if (ref.getSelect().getWhere().isPresent()) {
        ColumnMeta whereColumn = ref.getSelect().getWhere().get().compileColumn(srcTableMeta);
        derivedTable = new FilteredTableMeta(srcTableMeta, whereColumn);
      } else {
        derivedTable = srcTableMeta;
      }
      GlobalAggrTableMeta aggrTable = new GlobalAggrTableMeta(
          new KeyValueTableMeta(
              derivedTable,
              TableMeta.wholeTableKeys));
      // Columns have aggregations, recompile with an AggrTable.
      TableColumns tableColumns = SelectStatement.Select.tableColumns(aggrTable, ref.getSelect().getColumns());
      ReferencingDerivedTableMeta refTableMeta = new ReferencingDerivedTableMeta(
          aggrTable, tableColumns.getColumns(), srcTableMeta.getReverseForeignKey());
      tableMeta.addRef(new TableRef(ref.getName(), ref.getSelect(), refTableMeta));
      return new MetaStatementResult();
    }
  }
}
