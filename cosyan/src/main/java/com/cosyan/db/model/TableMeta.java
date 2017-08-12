package com.cosyan.db.model;

import java.io.DataInputStream;

import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.AggrTableReader;
import com.cosyan.db.io.TableReader.DerivedTableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.FilteredTableReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public abstract class TableMeta {

  protected static final ImmutableMap<String, ? extends ColumnMeta> wholeTableKeys = ImmutableMap.of("",
      ColumnMeta.TRUE_COLUMN);

  public abstract ImmutableMap<String, ? extends ColumnMeta> columns();

  protected abstract TableReader reader() throws ModelException;

  public abstract ImmutableMap<String, ? extends ColumnMeta> keyColumns();

  public abstract int indexOf(Ident ident);

  public static abstract class ExposedTableMeta extends TableMeta {
    @Override
    public abstract ExposedTableReader reader() throws ModelException;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MaterializedTableMeta extends ExposedTableMeta {
    private final String tableName;
    private final ImmutableMap<String, BasicColumn> columns;
    private final MetaRepo metaRepo;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return new MaterializedTableReader(
          new DataInputStream(metaRepo.open(this)),
          columns());
    }

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> keyColumns() {
      return wholeTableKeys;
    }

    @Override
    public int indexOf(Ident ident) {
      return columns().keySet().asList().indexOf(ident.getString());
    }
  }

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
    public ImmutableMap<String, ? extends ColumnMeta> keyColumns() {
      return wholeTableKeys;
    }

    @Override
    public int indexOf(Ident ident) {
      return columns().keySet().asList().indexOf(ident.getString());
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
    public ImmutableMap<String, ? extends ColumnMeta> keyColumns() {
      return wholeTableKeys;
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.indexOf(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class GroupByTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> keyColumns;
    private final ImmutableMap<String, ? extends ColumnMeta> valueColumns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return valueColumns;
    }

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> keyColumns() {
      return keyColumns;
    }

    @Override
    public TableReader reader() throws ModelException {
      return sourceTable.reader();
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.indexOf(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AggrTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final ImmutableList<AggrColumn> valueColumns;
    private final ColumnMeta havingColumn;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> keyColumns() {
      return wholeTableKeys;
    }

    @Override
    public TableReader reader() throws ModelException {
      return new AggrTableReader(sourceTable.reader(), sourceTable.keyColumns(), valueColumns, havingColumn);
    }

    @Override
    public int indexOf(Ident ident) {
      return sourceTable.keyColumns().size() + sourceTable.indexOf(ident);
    }
  }
}
