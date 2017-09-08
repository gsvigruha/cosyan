package com.cosyan.db.model;

import java.io.DataInputStream;
import java.util.Optional;

import com.cosyan.db.io.TableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.io.TableWriter.TableAppender;
import com.cosyan.db.io.TableWriter.TableDeleteAndCollector;
import com.cosyan.db.io.TableWriter.TableDeleter;
import com.cosyan.db.io.TableWriter.TableUpdater;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.Data;
import lombok.EqualsAndHashCode;

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

  protected ColumnMeta column(Ident ident, ImmutableMap<String, ? extends ColumnMeta> columns) throws ModelException {
    ColumnMeta column = columns.get(ident.getString());
    if (column == null) {
      throw new ModelException("Column '" + ident + "' not found in table.");
    }
    return column;
  }

  protected abstract TableReader reader() throws ModelException;

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
    public abstract ExposedTableReader reader() throws ModelException;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MaterializedTableMeta extends ExposedTableMeta {
    private final String tableName;
    private final ImmutableMap<String, BasicColumn> columns;
    private final MetaRepo metaRepo;

    private ImmutableMap<String, DerivedColumn> simpleChecks = ImmutableMap.of();
    private Optional<PrimaryKey> primaryKey = Optional.empty();
    private ImmutableMap<String, ForeignKey> foreignKeys = ImmutableMap.of();
    private ImmutableMap<String, ForeignKey> reverseForeignKeys = ImmutableMap.of();

    @Override
    public ImmutableMap<String, BasicColumn> columns() {
      return columns;
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return new MaterializedTableReader(
          new DataInputStream(metaRepo.open(this)),
          columns());
    }

    public TableAppender appender() throws ModelException {
      return new TableAppender(
          metaRepo.append(this),
          columns.values().asList(),
          metaRepo.collectUniqueIndexes(this),
          metaRepo.collectMultiIndexes(this),
          metaRepo.collectForeignIndexes(this),
          simpleChecks);
    }

    public TableDeleter deleter(DerivedColumn whereColumn) throws ModelException {
      return new TableDeleter(
          metaRepo.update(this),
          columns.values().asList(),
          whereColumn,
          metaRepo.collectUniqueIndexes(this),
          metaRepo.collectMultiIndexes(this),
          metaRepo.collectReverseForeignIndexes(this));
    }

    public TableUpdater updater(ImmutableMap<Integer, DerivedColumn> updateExprs, DerivedColumn whereColumn)
        throws ModelException {
      return new TableUpdater(
          new TableDeleteAndCollector(
              metaRepo.update(this),
              columns.values().asList(),
              updateExprs,
              whereColumn,
              metaRepo.collectUniqueIndexes(this),
              metaRepo.collectMultiIndexes(this),
              metaRepo.collectReverseForeignIndexes(this)),
          new TableAppender(
              metaRepo.append(this),
              columns.values().asList(),
              metaRepo.collectUniqueIndexes(this),
              metaRepo.collectMultiIndexes(this),
              metaRepo.collectForeignIndexes(this),
              simpleChecks));
    }

    @Override
    public int indexOf(Ident ident) {
      if (ident.isSimple()) {
        return indexOf(columns().keySet(), ident);
      } else {
        if (ident.head().equals(tableName)) {
          return indexOf(columns().keySet(), ident.tail());
        } else {
          return -1;
        }
      }
    }

    @Override
    public ColumnMeta column(Ident ident) throws ModelException {
      if (ident.isSimple()) {
        return column(ident, columns);
      } else {
        if (ident.head().equals(tableName)) {
          return column(ident.tail(), columns);
        } else {
          throw new ModelException("Table mismatch '" + ident.head() + "' instead of '" + tableName + "'.");
        }
      }
    }
  }
}
