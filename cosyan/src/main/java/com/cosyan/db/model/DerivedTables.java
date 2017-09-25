package com.cosyan.db.model;

import com.cosyan.db.io.Aggregation.GlobalAggrTableReader;
import com.cosyan.db.io.Aggregation.KeyValueAggrTableReader;
import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.DerivedTableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.FilteredTableReader;
import com.cosyan.db.io.TableReader.HashJoinTableReader;
import com.cosyan.db.io.TableReader.SortedTableReader;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.Tokens;
import com.cosyan.db.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DerivedTables {
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DerivedTableMeta extends ExposedTableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return new DerivedTableReader(sourceTable.reader(), columns());
    }

    @Override
    public int indexOf(Ident ident) {
      return indexOf(columns().keySet(), ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return column(ident, columns);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FilteredTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final ColumnMeta whereColumn;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return sourceTable.columns();
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return new FilteredTableReader(sourceTable.reader(), whereColumn);
    }

    @Override
    public int indexOf(Ident ident) throws ModelException {
      return sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class KeyValueTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> keyColumns;

    @Override
    public TableReader reader() throws ModelException {
      return sourceTable.reader();
    }

    @Override
    public int indexOf(Ident ident) {
      return indexOf(keyColumns.keySet(), ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class KeyValueAggrTableMeta extends TableMeta {
    private final KeyValueTableMeta sourceTable;
    private final ImmutableList<AggrColumn> aggrColumns;
    private final ColumnMeta havingColumn;

    @Override
    public TableReader reader() throws ModelException {
      return new KeyValueAggrTableReader(sourceTable.reader(), sourceTable.keyColumns, aggrColumns, havingColumn);
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.keyColumns.size() + sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class GlobalAggrTableMeta extends TableMeta {
    private final KeyValueTableMeta sourceTable;
    private final ImmutableList<AggrColumn> aggrColumns;
    private final ColumnMeta havingColumn;

    @Override
    public TableReader reader() throws ModelException {
      return new GlobalAggrTableReader(sourceTable.reader(), aggrColumns, havingColumn);
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.keyColumns.size() + sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }
  }
  
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SortedTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final ImmutableList<OrderColumn> orderColumns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return sourceTable.columns();
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return new SortedTableReader(sourceTable.reader(), orderColumns);
    }

    @Override
    public int indexOf(Ident ident) throws ModelException {
      return sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class JoinTableMeta extends ExposedTableMeta {
    private final Token joinType;
    private final ExposedTableMeta leftTable;
    private final ExposedTableMeta rightTable;
    private final ImmutableList<ColumnMeta> leftTableJoinColumns;
    private final ImmutableList<ColumnMeta> rightTableJoinColumns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return ImmutableMap.<String, ColumnMeta>builder()
          .putAll(leftTable.columns())
          .putAll(rightTable.columns())
          .build();
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      if (joinType.is(Tokens.INNER)) {
        return new HashJoinTableReader(leftTable.reader(), rightTable.reader(), leftTableJoinColumns,
            rightTableJoinColumns, true);
      } else {
        throw new ModelException("Unknown join type '" + joinType.getString() + "'.");
      }
    }

    @Override
    public int indexOf(Ident ident) throws ModelException {
      int leftIndex = leftTable.indexOf(ident);
      int rightIndex = rightTable.indexOf(ident);
      if (leftIndex >= 0 && rightIndex < 0) {
        return leftIndex;
      }
      if (leftIndex < 0 && rightIndex >= 0) {
        return leftTable.columns().size() + rightIndex;
      }
      if (leftIndex >= 0 && rightIndex >= 0) {
        throw new ModelException("Ambiguous column reference '" + ident + "'.");
      }
      return -1;
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      int leftIndex = leftTable.indexOf(ident);
      int rightIndex = rightTable.indexOf(ident);
      if (leftIndex >= 0 && rightIndex < 0) {
        return leftTable.column(ident);
      }
      if (leftIndex < 0 && rightIndex >= 0) {
        return rightTable.column(ident);
      }
      if (leftIndex >= 0 && rightIndex >= 0) {
        throw new ModelException("Ambiguous column reference '" + ident + "'.");
      }
      throw new ModelException("Column '" + ident + "' not found in table.");
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AliasedTableMeta extends ExposedTableMeta {
    private final Ident ident;
    private final ExposedTableMeta sourceTable;

    public AliasedTableMeta(Ident ident, ExposedTableMeta sourceTable) {
      this.ident = ident;
      this.sourceTable = sourceTable;
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return sourceTable.reader();
    }

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return sourceTable.columns();
    }

    @Override
    public int indexOf(Ident ident) throws ModelException {
      if (ident.isSimple()) {
        return sourceTable.indexOf(ident);
      } else {
        if (ident.head().equals(this.ident.getString())) {
          return sourceTable.indexOf(ident.tail());
        } else {
          return -1;
        }
      }
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      if (ident.isSimple()) {
        return sourceTable.column(ident);
      } else {
        if (ident.head().equals(this.ident.getString())) {
          return sourceTable.column(ident.tail());
        } else {
          throw new ModelException("Column '" + ident + "' not found in table.");
        }
      }
    }
  }
}
