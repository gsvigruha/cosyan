package com.cosyan.db.transaction;

import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.util.Util;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import lombok.Data;

public class MetaResources {

  public static interface Resource {
    public boolean isWrite();

    public String getResourceId();
  }

  @Data
  public static class TableMetaResource implements Resource {
    private final MaterializedTableMeta tableMeta;
    private final boolean write;

    public TableMetaResource merge(TableMetaResource other) {
      if (isWrite())
        return this;
      else
        return other;
    }

    @Override
    public String getResourceId() {
      return tableMeta.getTableName();
    }
  }

  @Data
  public static class IndexMetaResource implements Resource {
    private final TableIndex tableIndex;
    private final boolean write;

    public IndexMetaResource merge(IndexMetaResource other) {
      if (isWrite())
        return this;
      else
        return other;
    }

    @Override
    public String getResourceId() {
      throw new UnsupportedOperationException();
    }
  }

  private final ImmutableMap<String, TableMetaResource> tables;
  private final ImmutableMap<String, IndexMetaResource> indexes;

  public MetaResources(ImmutableMap<String, TableMetaResource> tables,
      ImmutableMap<String, IndexMetaResource> indexes) {
    this.tables = tables;
    this.indexes = indexes;
  }

  public MetaResources merge(MetaResources other) {
    return new MetaResources(
        Util.merge(this.tables, other.tables, TableMetaResource::merge),
        Util.merge(this.indexes, other.indexes, IndexMetaResource::merge));
  }

  public Iterable<Resource> all() {
    return Iterables.concat(tables.values(), indexes.values());
  }

  public Iterable<TableMetaResource> tables() {
    return tables.values();
  }

  public static MetaResources readTable(MaterializedTableMeta tableMeta) {
    return new MetaResources(ImmutableMap.of(tableMeta.getTableName(), new TableMetaResource(tableMeta, false)),
        ImmutableMap.of());
  }

  public static MetaResources writeTable(MaterializedTableMeta tableMeta) {
    return new MetaResources(ImmutableMap.of(tableMeta.getTableName(), new TableMetaResource(tableMeta, true)),
        ImmutableMap.of());
  }

  public static MetaResources empty() {
    return new MetaResources(ImmutableMap.of(), ImmutableMap.of());
  }
}
