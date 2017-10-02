package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.io.AggrReader.GlobalAggrTableReader;
import com.cosyan.db.io.AggrReader.KeyValueAggrTableReader;
import com.cosyan.db.io.JoinTableReader.HashJoinTableReader;
import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.DerivedTableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.FilteredTableReader;
import com.cosyan.db.io.TableReader.SortedTableReader;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.Tokens;
import com.cosyan.db.sql.Tokens.Token;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
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
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new DerivedTableReader(sourceTable.reader(resources), columns());
    }

    @Override
    public int indexOf(Ident ident) {
      return indexOf(columns().keySet(), ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return column(ident, columns);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
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
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new FilteredTableReader(sourceTable.reader(resources), whereColumn);
    }

    @Override
    public int indexOf(Ident ident) throws ModelException {
      return sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class KeyValueTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> keyColumns;

    @Override
    public TableReader reader(Resources resources) throws IOException {
      return sourceTable.reader(resources);
    }

    @Override
    public int indexOf(Ident ident) {
      return indexOf(keyColumns.keySet(), ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class KeyValueAggrTableMeta extends TableMeta {
    private final KeyValueTableMeta sourceTable;
    private final ImmutableList<AggrColumn> aggrColumns;
    private final ColumnMeta havingColumn;

    @Override
    public TableReader reader(Resources resources) throws IOException {
      return new KeyValueAggrTableReader(
          sourceTable.reader(resources),
          sourceTable.keyColumns,
          aggrColumns,
          havingColumn);
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.keyColumns.size() + sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class GlobalAggrTableMeta extends TableMeta {
    private final KeyValueTableMeta sourceTable;
    private final ImmutableList<AggrColumn> aggrColumns;
    private final ColumnMeta havingColumn;

    @Override
    public TableReader reader(Resources resources) throws IOException {
      return new GlobalAggrTableReader(sourceTable.reader(resources), aggrColumns, havingColumn);
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.keyColumns.size() + sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
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
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new SortedTableReader(sourceTable.reader(resources), orderColumns);
    }

    @Override
    public int indexOf(Ident ident) throws ModelException {
      return sourceTable.indexOf(ident);
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      return sourceTable.column(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
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
    public ExposedTableReader reader(Resources resources) throws IOException {
      if (joinType.is(Tokens.INNER)) {
        return new HashJoinTableReader(
            leftTable.reader(resources),
            rightTable.reader(resources),
            leftTableJoinColumns,
            rightTableJoinColumns,
            /* mainTableFirst= */true,
            /* innerJoin= */true);
      } else if (joinType.is(Tokens.LEFT)) {
        return new HashJoinTableReader(
            leftTable.reader(resources),
            rightTable.reader(resources),
            leftTableJoinColumns,
            rightTableJoinColumns,
            /* mainTableFirst= */true,
            /* innerJoin= */false);
      } else if (joinType.is(Tokens.RIGHT)) {
        return new HashJoinTableReader(
            rightTable.reader(resources),
            leftTable.reader(resources),
            rightTableJoinColumns,
            leftTableJoinColumns,
            /* mainTableFirst= */false,
            /* innerJoin= */false);
      } else {
        // TODO remove this and resolve in compilation time.
        throw new RuntimeException("Unknown join type '" + joinType.getString() + "'.");
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

    @Override
    public MetaResources readResources() {
      return leftTable.readResources().merge(rightTable.readResources());
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
    public ExposedTableReader reader(Resources resources) throws IOException {
      return sourceTable.reader(resources);
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

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }
}
