package com.cosyan.db.model;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import com.cosyan.db.io.ReferencedMultiTableReader;
import com.cosyan.db.io.TableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.ColumnMeta.IterableColumn;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class References {
  @Data
  public static class Column {
    private final ColumnMeta meta;
    private final int index;

    public Column shift(int i) {
      return new Column(meta, index + i);
    }

    public boolean usesRefValues() {
      return false;
    }

    public ColumnMeta toMeta() {
      final int index = getIndex();
      return new DerivedColumnWithDeps(getMeta().getType(), new TableDependencies()) {
        @Override
        public Object getValue(SourceValues values) {
          return values.sourceValue(index);
        }
      };
    }
  }

  public static abstract class MaterializedColumn extends Column {
    public MaterializedColumn(ColumnMeta meta, int index) {
      super(meta, index);
    }

    @Override
    public BasicColumn getMeta() {
      return (BasicColumn) super.getMeta();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SimpleMaterializedColumn extends MaterializedColumn {

    protected final MaterializedTableMeta tableMeta;

    public SimpleMaterializedColumn(MaterializedTableMeta tableMeta, BasicColumn column, int index) {
      super(column, index);
      this.tableMeta = tableMeta;
    }

    public MaterializedTableMeta table() {
      return tableMeta;
    }
  }

  public interface ReferencingColumn {

    ReferencedTableMeta getTableMeta();

    String tableNameWithChain();

    int getIndex();

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MultiReferencingColumn extends Column implements ReferencingColumn {

    private final ReferencedTableMeta tableMeta;

    public MultiReferencingColumn(ReferencedTableMeta tableMeta, ColumnMeta column, int index) throws ModelException {
      super(column, index);
      this.tableMeta = tableMeta;
    }

    public String tableNameWithChain() {
      return tableMeta.tableNameWithChain();
    }

    @Override
    public boolean usesRefValues() {
      return true;
    }

    public Iterable<Ref> foreignKeyChain() {
      return tableMeta.foreignKeyChain();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SimpleReferencingColumn extends Column implements ReferencingColumn {

    private final ReferencedTableMeta tableMeta;
    private final BasicColumn originalColumn;

    public SimpleReferencingColumn(ReferencedTableMeta tableMeta, BasicColumn column, int index) throws ModelException {
      super(column, index);
      this.tableMeta = tableMeta;
      this.originalColumn = column;
    }

    public String tableNameWithChain() {
      return tableMeta.tableNameWithChain();
    }

    @Override
    public boolean usesRefValues() {
      return true;
    }

    public Iterable<Ref> foreignKeyChain() {
      return tableMeta.foreignKeyChain();
    }

    public BasicColumn getOriginalMeta() {
      return originalColumn;
    }

    @Override
    public ColumnMeta toMeta() {
      TableDependencies tableDependencies = new TableDependencies();
      tableDependencies.addTableDependency(this);
      return new DerivedColumnWithDeps(getMeta().getType(), tableDependencies) {
        @Override
        public Object getValue(SourceValues values) throws IOException {
          return values.refTableValue(SimpleReferencingColumn.this);
        }
      };
    }
  }

  public static abstract class ReferencedTableMeta extends TableMeta {
    public abstract String tableNameWithChain();

    public abstract ColumnMeta transform(BasicColumn column) throws ModelException;

    public abstract Iterable<Ref> foreignKeyChain();

    public abstract ReferencedTableMeta getParent();

    public static TableMeta getRefTable(
        @Nullable ReferencedTableMeta parent,
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedSimpleTableMeta extends ReferencedTableMeta {

    @Nullable
    private final ReferencedTableMeta parent;
    private final ForeignKey foreignKey;

    public ReferencedSimpleTableMeta(ReferencedTableMeta parent, ForeignKey foreignKey) {
      this.parent = parent;
      this.foreignKey = foreignKey;
    }

    public Iterable<Ref> foreignKeyChain() {
      return parent == null ? ImmutableList.of(foreignKey)
          : ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(foreignKey).build();
    }

    @Override
    public String tableNameWithChain() {
      return parent == null ? foreignKey.getName() : parent.tableNameWithChain() + "." + foreignKey.getName();
    }

    @Override
    public BasicColumn transform(BasicColumn column) {
      return column;
    }

    @Override
    protected Column getColumn(Ident ident) throws ModelException {
      SimpleMaterializedColumn column = foreignKey.getRefTable().getColumn(ident);
      if (column == null) {
        return null;
      }
      return new SimpleReferencingColumn(this, column.getMeta(), column.getIndex());
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return getRefTable(
          this,
          tableNameWithChain(),
          ident.getString(),
          foreignKey.getRefTable().foreignKeys(),
          foreignKey.getRefTable().reverseForeignKeys());
    }

    @Override
    protected TableReader reader(Resources resources) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public MetaResources readResources() {
      MetaResources parentResources = parent == null ? MetaResources.empty() : parent.readResources();
      return parentResources.merge(MetaResources.readTable(foreignKey.getRefTable()));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedMultiTableMeta extends ReferencedTableMeta {

    @Nullable
    private final ReferencedTableMeta parent;
    private final ReverseForeignKey reverseForeignKey;
    private final KeyValueTableMeta proxyTable;

    private ImmutableList<AggrColumn> columns;

    public ReferencedMultiTableMeta(ReferencedTableMeta parent, ReverseForeignKey reverseForeignKey) {
      this.parent = parent;
      this.reverseForeignKey = reverseForeignKey;
      MaterializedTableMeta sourceTable = getReverseForeignKey().getRefTable();
      this.proxyTable = new KeyValueTableMeta(
          sourceTable,
          TableMeta.wholeTableKeys);
    }

    public Iterable<Ref> foreignKeyChain() {
      return parent == null ? ImmutableList.of(reverseForeignKey)
          : ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(reverseForeignKey).build();
    }

    @Override
    public String tableNameWithChain() {
      return parent == null ? reverseForeignKey.getName()
          : parent.tableNameWithChain() + "." + reverseForeignKey.getName();
    }

    @Override
    public ColumnMeta transform(BasicColumn column) throws ModelException {
      return new IterableColumn(column);
    }

    @Override
    protected Column getColumn(Ident ident) throws ModelException {
      return proxyTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return getRefTable(
          this,
          tableNameWithChain(),
          ident.getString(),
          reverseForeignKey.getRefTable().foreignKeys(),
          reverseForeignKey.getRefTable().reverseForeignKeys());
    }

    @Override
    public ReferencedMultiTableReader reader(Resources resources) throws IOException {
      return new ReferencedMultiTableReader(
          reverseForeignKey.getRefTable(),
          resources,
          columns);
    }

    @Override
    public MetaResources readResources() {
      return MetaResources.readTable(reverseForeignKey.getRefTable())
          .merge(DerivedTables.resourcesFromColumns(columns));
    }
  }
}
