package com.cosyan.db.model;

import java.io.DataInputStream;

import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.DerivedTableReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public abstract class TableMeta {

  public abstract ImmutableMap<String, ? extends ColumnMeta> columns();

  public abstract TableReader reader() throws ModelException;

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MaterializedTableMeta extends TableMeta {
    private final String tableName;
    private final ImmutableMap<String, BasicColumn> columns;
    private final MetaRepo metaRepo;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public TableReader reader() throws ModelException {
      return new MaterializedTableReader(
          new DataInputStream(metaRepo.open(this)),
          columns());
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DerivedTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public TableReader reader() throws ModelException {
      return new DerivedTableReader(sourceTable.reader(), columns());
    }
  }
}
