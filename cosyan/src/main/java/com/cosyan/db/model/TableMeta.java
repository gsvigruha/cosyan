package com.cosyan.db.model;

import java.io.IOException;

import javax.annotation.Nullable;

import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.References.Column;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public abstract class TableMeta implements CompiledObject {

  public static final ImmutableMap<String, ColumnMeta> wholeTableKeys = ImmutableMap.of("",
      ColumnMeta.TRUE_COLUMN);

  public Column column(Ident ident) throws ModelException {
    Column column = getColumn(ident);
    if (column == null) {
      throw new ModelException(String.format("Column '%s' not found in table.", ident));
    }
    return column;
  }

  public boolean hasColumn(Ident ident) {
    try {
      return getColumn(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  public TableMeta table(Ident ident) throws ModelException {
    TableMeta table = getTable(ident);
    if (table == null) {
      throw new ModelException(String.format("Table reference '%s' not found.", ident));
    }
    return table;
  }

  public boolean hasTable(Ident ident) {
    try {
      return getTable(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  @Nullable
  protected abstract Column getColumn(Ident ident) throws ModelException;

  @Nullable
  protected abstract TableMeta getTable(Ident ident) throws ModelException;

  protected abstract TableReader reader(Resources resources) throws IOException;

  public abstract MetaResources readResources();

  protected int indexOf(ImmutableSet<String> keys, Ident ident) {
    return keys.asList().indexOf(ident.getString());
  }

  public static abstract class ExposedTableMeta extends TableMeta {
    public abstract ImmutableList<String> columnNames();

    @Override
    public abstract ExposedTableReader reader(Resources resources) throws IOException;
  }
}
