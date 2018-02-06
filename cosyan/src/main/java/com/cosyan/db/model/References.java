package com.cosyan.db.model;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.MultiFilteredTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables.GlobalAggrTableMeta;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class References {

  public static interface ReferencingTable {
    
    public Iterable<Ref> foreignKeyChain();

    public ReferencingTable getParent();

    public MetaResources readResources();

    public Object[] values(Object[] sourceValues, Resources resources) throws IOException;
  }

  public static TableMeta getRefTable(
      ReferencingTable parent,
      String tableName,
      String key,
      Map<String, ForeignKey> foreignKeys,
      Map<String, ReverseForeignKey> reverseForeignKeys) throws ModelException {
    if (foreignKeys.containsKey(key)) {
      return new ReferencedSimpleTableMeta(parent, foreignKeys.get(key));
    } else if (reverseForeignKeys.containsKey(key)) {
      return new ReferencedMultiTableMeta(parent, reverseForeignKeys.get(key));
    }
    throw new ModelException(String.format("Reference '%s' not found in table '%s'.", key, tableName));
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedSimpleTableMeta extends TableMeta implements ReferencingTable {

    @Nullable
    private final ReferencingTable parent;
    private final ForeignKey foreignKey;

    public ReferencedSimpleTableMeta(ReferencingTable parent, ForeignKey foreignKey) {
      this.parent = parent;
      this.foreignKey = foreignKey;
    }

    public Iterable<Ref> foreignKeyChain() {
      return parent == null ? ImmutableList.of(foreignKey)
          : ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(foreignKey).build();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      BasicColumn column = foreignKey.getRefTable().column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this, column);
      return new IndexColumn(this, column.getIndex(), column.getType(), deps);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return References.getRefTable(
          this,
          foreignKey.getTable().tableName(),
          ident.getString(),
          foreignKey.getRefTable().foreignKeys(),
          foreignKey.getRefTable().reverseForeignKeys());
    }

    @Override
    public MetaResources readResources() {
      MetaResources parentResources = parent == null ? MetaResources.empty() : parent.readResources();
      return parentResources.merge(MetaResources.readTable(foreignKey.getRefTable()));
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources) throws IOException {
      Object[] parentValues = parent.values(sourceValues, resources);
      Object key = parentValues[foreignKey.getColumn().getIndex()];
      IndexReader index = resources.getIndex(foreignKey);
      long filePointer = index.get(key)[0];
      SeekableTableReader reader = resources.reader(foreignKey.getRefTable().tableName());
      return reader.get(filePointer).getValues();
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedMultiTableMeta extends IterableTableMeta implements ReferencingTable {

    @Nullable
    private final ReferencingTable parent;
    private final ReverseForeignKey reverseForeignKey;
    private final MaterializedTableMeta sourceTable;

    public ReferencedMultiTableMeta(ReferencingTable parent, ReverseForeignKey reverseForeignKey) {
      this.parent = parent;
      this.reverseForeignKey = reverseForeignKey;
      this.sourceTable = getReverseForeignKey().getRefTable();
    }

    public Iterable<Ref> foreignKeyChain() {
      return parent == null ? ImmutableList.of(reverseForeignKey)
          : ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(reverseForeignKey).build();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      BasicColumn column = reverseForeignKey.getRefTable().column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this, column);
      return new IndexColumn(this, column.getIndex(), column.getType(), deps);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return References.getRefTable(
          this,
          reverseForeignKey.getTable().tableName(),
          ident.getString(),
          reverseForeignKey.getRefTable().foreignKeys(),
          reverseForeignKey.getRefTable().reverseForeignKeys());
    }

    @Override
    public MetaResources readResources() {
      MetaResources parentResources = parent == null ? MetaResources.empty() : parent.readResources();
      return parentResources.merge(MetaResources.readTable(reverseForeignKey.getRefTable()));
    }

    @Override
    public IterableTableReader reader(final Object key, Resources resources) throws IOException {

      String table = reverseForeignKey.getRefTable().tableName();
      final IndexReader index = resources.getIndex(reverseForeignKey);
      MultiFilteredTableReader reader = new MultiFilteredTableReader(resources.reader(table), ColumnMeta.TRUE_COLUMN,
          resources) {

        @Override
        protected void readPositions() throws IOException {
          positions = index.get(key);
        }
      };

      return new IterableTableReader() {

        @Override
        public void close() throws IOException {
          reader.close();
        }

        @Override
        public Object[] next() throws IOException {
          Object[] values = reader.next();
          if (values == null) {
            return null;
          }
          return values;
        }
      };
    }
  }

  public static class ReferencedAggrTableMeta extends GlobalAggrTableMeta {
    private final ReverseForeignKey reverseForeignKey;

    public ReferencedAggrTableMeta(
        KeyValueTableMeta sourceTable,
        ReverseForeignKey reverseForeignKey) {
      super(sourceTable);
      this.reverseForeignKey = reverseForeignKey;
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources) throws IOException {
      Object key = sourceValues[reverseForeignKey.getColumn().getIndex()];
      IterableTableReader reader = reader(key, resources);
      Object[] values = reader.next();
      reader.close();
      Object[] result = new Object[values.length - 1];
      System.arraycopy(values, 1, result, 0, result.length);
      return result;
    }
  }
}
