package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.io.AggrReader.GlobalAggrTableReader;
import com.cosyan.db.io.AggrReader.KeyValueAggrTableReader;
import com.cosyan.db.io.JoinTableReader.HashJoinTableReader;
import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.DerivedTableReader;
import com.cosyan.db.io.TableReader.DistinctTableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.FilteredTableReader;
import com.cosyan.db.io.TableReader.IndexFilteredTableReader;
import com.cosyan.db.io.TableReader.SortedTableReader;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.References.Column;
import com.cosyan.db.model.References.ReferencedTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DerivedTables {

  protected static MetaResources resourcesFromColumns(Iterable<? extends ColumnMeta> columns) {
    MetaResources resources = MetaResources.empty();
    for (ColumnMeta columnMeta : columns) {
      resources = resources.merge(
          MaterializedTableMeta.readResources(columnMeta.tableDependencies().getDeps().values()));
    }
    return resources;
  }

  private static MetaResources resourcesFromColumn(ColumnMeta column) {
    return MaterializedTableMeta.readResources(column.tableDependencies().getDeps().values());
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DerivedTableMeta extends ExposedTableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    public ImmutableList<String> columnNames() {
      return columns.keySet().asList();
    }

    @Override
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new DerivedTableReader(sourceTable.reader(resources), columns);
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return new Column(columns.get(ident.getString()), indexOf(columns.keySet(), ident));
    }

    @Override
    protected ReferencedTableMeta getRefTable(Ident ident) throws ModelException {
      return null;
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumns(columns.values()));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FilteredTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final ColumnMeta whereColumn;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new FilteredTableReader(sourceTable.reader(resources), whereColumn);
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumn(whereColumn));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IndexFilteredTableMeta extends ExposedTableMeta {
    private final MaterializedTableMeta sourceTable;
    private final ColumnMeta whereColumn;
    private final VariableEquals clause;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new IndexFilteredTableReader(
          sourceTable.reader(resources),
          whereColumn,
          clause);
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumn(whereColumn));
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
    public Column getColumn(Ident ident) throws ModelException {
      if (keyColumns.containsKey(ident.getString())) {
        return new Column(keyColumns.get(ident.getString()), indexOf(keyColumns.keySet(), ident));
      } else {
        return null;
      }
    }

    @Override
    protected ReferencedTableMeta getRefTable(Ident ident) throws ModelException {
      return null;
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumns(keyColumns.values()));
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
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.column(ident).shift(sourceTable.keyColumns.size());
    }

    @Override
    protected ReferencedTableMeta getRefTable(Ident ident) throws ModelException {
      return null;
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
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.column(ident).shift(sourceTable.keyColumns.size());
    }

    @Override
    protected ReferencedTableMeta getRefTable(Ident ident) throws ModelException {
      return null;
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
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new SortedTableReader(sourceTable.reader(resources), orderColumns);
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DistinctTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ExposedTableReader reader(Resources resources) throws IOException {
      return new DistinctTableReader(sourceTable.reader(resources));
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
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
    public ImmutableList<String> columnNames() {
      return ImmutableList.<String>builder()
          .addAll(leftTable.columnNames())
          .addAll(rightTable.columnNames())
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
    public Column getColumn(Ident ident) throws ModelException {
      boolean presentInLeftTable = leftTable.hasColumn(ident);
      boolean presentInRightTable = rightTable.hasColumn(ident);
      if (presentInLeftTable && presentInRightTable) {
        throw new ModelException("Ambiguous column reference '" + ident + "'.");
      }
      if (presentInLeftTable) {
        return leftTable.getColumn(ident);
      }
      if (presentInRightTable) {
        return rightTable.getColumn(ident).shift(leftTable.columnNames().size());
      }
      throw new ModelException("Column '" + ident + "' not found in table.");
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      boolean presentInLeftTable = leftTable.hasTable(ident);
      boolean presentInRightTable = rightTable.hasTable(ident);
      if (presentInLeftTable && presentInRightTable) {
        throw new ModelException("Ambiguous table reference '" + ident + "'.");
      }
      if (presentInLeftTable) {
        return leftTable.getRefTable(ident);
      }
      if (presentInRightTable) {
        return new ShiftedTableMeta(rightTable.getRefTable(ident), leftTable.columnNames().size());
      }
      throw new ModelException("Table reference '" + ident + "' not found.");
    }

    @Override
    public MetaResources readResources() {
      return leftTable.readResources()
          .merge(rightTable.readResources())
          .merge(resourcesFromColumns(leftTableJoinColumns))
          .merge(resourcesFromColumns(rightTableJoinColumns));
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
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      if (this.ident.getString().equals(ident.getString())) {
        return sourceTable;
      }
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ShiftedTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final int shift;

    public ShiftedTableMeta(TableMeta sourceTable, int shift) {
      this.sourceTable = sourceTable;
      this.shift = shift;
    }

    @Override
    public TableReader reader(Resources resources) throws IOException {
      return sourceTable.reader(resources);
    }

    @Override
    public Column getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident).shift(shift);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }
}
