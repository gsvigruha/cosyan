package com.cosyan.db.model;

import java.io.IOException;
import java.util.Map;

import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.lang.sql.SyntaxTree.Ident;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public abstract class TableMeta {

  public static final ImmutableMap<String, ColumnMeta> wholeTableKeys = ImmutableMap.of("",
      ColumnMeta.TRUE_COLUMN);

  /**
   * @param ident
   * @return The ColumnMeta identified by <code>ident</code> in this table.
   * @throws ModelException
   *           if <code>ident</code> is not present or ambiguous.
   */
  public abstract ColumnMeta column(Ident ident) throws ModelException;

  protected ColumnMeta column(Ident ident, Map<String, ? extends ColumnMeta> columns) throws ModelException {
    ColumnMeta column = columns.get(ident.getString());
    if (column == null) {
      throw new ModelException("Column '" + ident + "' not found in table.");
    }
    return column;
  }

  protected abstract TableReader reader(Resources resources) throws IOException;

  public abstract MetaResources readResources();

  /**
   * @param ident
   * @return The index of <code>ident</code> in this table or -1 if not present.
   * @throws ModelException
   *           if the <code>ident</code> is ambiguous in this table.
   */
  public abstract int indexOf(Ident ident) throws ModelException;

  protected int indexOf(ImmutableSet<String> keys, Ident ident) {
    return keys.asList().indexOf(ident.getString());
  }

  public static abstract class ExposedTableMeta extends TableMeta {
    public abstract ImmutableMap<String, ? extends ColumnMeta> columns();

    @Override
    public abstract ExposedTableReader reader(Resources resources) throws IOException;
  }
}
