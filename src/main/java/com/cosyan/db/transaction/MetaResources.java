/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.transaction;

import java.util.HashMap;
import java.util.Map;

import com.cosyan.db.meta.DBObject;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.View;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.util.Util;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

public class MetaResources {

  @Data
  public static class Resource {
    private final String resourceId;
    private final boolean write;

    public Resource merge(Resource other) {
      assert (this.resourceId.equals(other.resourceId));
      return new Resource(
          this.resourceId,
          this.write || other.write);
    }
  }

  @Data
  public static class TableMetaResource {
    private final DBObject object;
    private final boolean select;
    private final boolean insert;
    private final boolean delete;
    private final boolean update;
    private final boolean foreignIndexes;
    private final boolean reverseForeignIndexes;
    private final boolean meta;

    public TableMetaResource merge(TableMetaResource other) {
      assert (this.object == other.object);
      return new TableMetaResource(
          this.object,
          this.select || other.select,
          this.insert || other.insert,
          this.delete || other.delete,
          this.update || other.update,
          this.foreignIndexes || other.foreignIndexes,
          this.reverseForeignIndexes || other.reverseForeignIndexes,
          this.meta || other.meta);
    }

    public boolean write() {
      return insert || delete || update;
    }

    public ImmutableMap<String, Resource> resources() {
      Map<String, Resource> builder = new HashMap<>();
      String fullName = object.fullName();
      builder.put(fullName, new Resource(fullName, write()));
      if (reverseForeignIndexes) {
        for (ReverseForeignKey foreignKey : ((MaterializedTable) object).reverseForeignKeys().values()) {
          String refTableName = foreignKey.getRefTable().fullName();
          builder.put(refTableName, new Resource(refTableName, /* write= */false));
        }
      }
      return ImmutableMap.copyOf(builder);
    }
  }

  private final ImmutableMap<String, TableMetaResource> tables;
  private final ImmutableCollection<Resource> resources;

  public MetaResources(ImmutableMap<String, TableMetaResource> tables) {
    this.tables = tables;
    this.resources = Util.merge(
        Util.mapValues(tables, TableMetaResource::resources).values(),
        Resource::merge).values();
  }

  public MetaResources merge(MetaResources other) {
    return new MetaResources(
        Util.merge(this.tables, other.tables, TableMetaResource::merge));
  }

  public Iterable<TableMetaResource> tables() {
    return tables.values();
  }

  public static MetaResources readTable(DBObject tableMeta) {
    return new MetaResources(ImmutableMap.of(
        tableMeta.fullName(),
        new TableMetaResource(
            tableMeta,
            /* select= */true,
            /* insert= */false,
            /* delete= */false,
            /* update= */false,
            /* foreignIndexes= */false,
            /* reverseForeignIndexes= */false,
            /* meta= */false)));
  }

  public static MetaResources updateTable(MaterializedTable tableMeta) {
    return new MetaResources(ImmutableMap.of(
        tableMeta.fullName(),
        new TableMetaResource(
            tableMeta,
            /* select= */false,
            /* insert= */false,
            /* delete= */false,
            /* update= */true,
            /* foreignIndexes= */true,
            /* reverseForeignIndexes= */true,
            /* meta= */false)))
                .merge(tableMeta.ruleDependenciesReadResources())
                .merge(tableMeta.reverseRuleDependenciesReadResources());
  }

  public static MetaResources insertIntoTable(MaterializedTable tableMeta) {
    return new MetaResources(ImmutableMap.of(
        tableMeta.fullName(),
        new TableMetaResource(
            tableMeta,
            /* select= */false,
            /* insert= */true,
            /* delete= */false,
            /* update= */false,
            /* foreignIndexes= */true, // New records have to satisfy foreign key constraints.
            /* reverseForeignIndexes= */false,
            /* meta= */false)))
                .merge(tableMeta.ruleDependenciesReadResources())
                .merge(tableMeta.reverseRuleDependenciesReadResources());
  }

  public static MetaResources deleteFromTable(MaterializedTable tableMeta) {
    return new MetaResources(ImmutableMap.of(
        tableMeta.fullName(),
        new TableMetaResource(
            tableMeta,
            /* select= */false,
            /* insert= */false,
            /* delete= */true,
            /* update= */false,
            /* foreignIndexes= */false,
            /* reverseForeignIndexes= */true, // Cannot delete records referenced by foreign keys.
            /* meta= */false)))
                .merge(tableMeta.reverseRuleDependenciesReadResources());
  }

  public static MetaResources tableMeta(MaterializedTable tableMeta) {
    return new MetaResources(ImmutableMap.of(
        tableMeta.fullName(),
        new TableMetaResource(
            tableMeta,
            /* select= */true,
            /* insert= */true,
            /* delete= */true,
            /* update= */true,
            /* foreignIndexes= */true,
            /* reverseForeignIndexes= */true,
            /* meta= */true)));
  }

  public static MetaResources viewMeta(View view) {
    return new MetaResources(ImmutableMap.of(
        view.fullName(),
        new TableMetaResource(
            view,
            /* select= */true,
            /* insert= */false,
            /* delete= */false,
            /* update= */false,
            /* foreignIndexes= */false,
            /* reverseForeignIndexes= */false,
            /* meta= */true)));
  }

  public static MetaResources empty() {
    return new MetaResources(ImmutableMap.of());
  }

  public Iterable<Resource> all() {
    return resources;
  }
}
