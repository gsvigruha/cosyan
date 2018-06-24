package com.cosyan.db.model;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public abstract class TableMeta implements CompiledObject {

  public static final ImmutableMap<String, ColumnMeta> wholeTableKeys = ImmutableMap.of("",
      ColumnMeta.TRUE_COLUMN);

  public IndexColumn column(Ident ident) throws ModelException {
    IndexColumn column = getColumn(ident);
    if (column == null) {
      throw new ModelException(String.format("Column '%s' not found in table.", ident), ident);
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

  public abstract Object[] values(Object[] sourceValues, Resources resources) throws IOException;

  public TableMeta table(Ident ident) throws ModelException {
    TableMeta table = getRefTable(ident);
    if (table == null) {
      throw new ModelException(String.format("Table reference '%s' not found.", ident), ident);
    }
    return table;
  }

  public boolean hasTable(Ident ident) {
    try {
      return getRefTable(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  @Nullable
  protected abstract IndexColumn getColumn(Ident ident) throws ModelException;

  @Nullable
  protected abstract TableMeta getRefTable(Ident ident) throws ModelException;

  public abstract MetaResources readResources();

  protected int indexOf(ImmutableSet<String> keys, Ident ident) {
    return keys.asList().indexOf(ident.getString());
  }

  public static abstract class IterableTableMeta extends TableMeta {

    public IterableTableReader reader(Resources resources) throws IOException {
      return reader(null, resources);
    }

    public abstract IterableTableReader reader(Object key, Resources resources) throws IOException;

    // Iterable tables cannot override this function.
    public final Object[] values(Object[] sourceValues, Resources resources) throws IOException {
      return sourceValues;
    }
  }

  public static abstract class ExposedTableMeta extends IterableTableMeta {
    public abstract ImmutableList<String> columnNames();

    public abstract ImmutableList<DataType<?>> columnTypes();
  }

  protected AtomicBoolean cancelled = new AtomicBoolean(false);
}
