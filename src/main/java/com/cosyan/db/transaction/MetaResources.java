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
import java.util.stream.Collectors;

import com.cosyan.db.meta.DBObject;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.View.TopLevelView;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.util.Util;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

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
  public static abstract class MetaResource {
    public abstract ImmutableMap<String, Resource> resources();

    public abstract MetaResource merge(MetaResource o);

    public abstract DBObject getObject();

    public abstract boolean write();

    public abstract boolean isSelect();

    public abstract boolean isDelete();

    public abstract boolean isUpdate();

    public abstract boolean isInsert();

    public abstract boolean isMeta();

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ViewMetaResource extends MetaResource {
    private final TopLevelView view;
    private final boolean meta;

    @Override
    public ImmutableMap<String, Resource> resources() {
      return ImmutableMap.of(view.fullName(), new Resource(view.fullName(), false));
    }

    @Override
    public ViewMetaResource merge(MetaResource o) {
      ViewMetaResource other = (ViewMetaResource) o;
      assert (this.view == other.view);
      return new ViewMetaResource(
          this.view,
          this.meta || other.meta);
    }

    @Override
    public DBObject getObject() {
      return view;
    }

    @Override
    public boolean write() {
      return false;
    }

    @Override
    public boolean isSelect() {
      return true;
    }

    @Override
    public boolean isDelete() {
      return false;
    }

    @Override
    public boolean isUpdate() {
      return false;
    }

    @Override
    public boolean isInsert() {
      return false;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableMetaResource extends MetaResource {
    private final MaterializedTable table;
    private final boolean select;
    private final boolean insert;
    private final boolean delete;
    private final boolean update;
    private final boolean foreignIndexes;
    private final boolean reverseForeignIndexes;
    private final boolean meta;

    @Override
    public TableMetaResource merge(MetaResource o) {
      TableMetaResource other = (TableMetaResource) o;
      assert (this.table == other.table);
      return new TableMetaResource(
          this.table,
          this.select || other.select,
          this.insert || other.insert,
          this.delete || other.delete,
          this.update || other.update,
          this.foreignIndexes || other.foreignIndexes,
          this.reverseForeignIndexes || other.reverseForeignIndexes,
          this.meta || other.meta);
    }

    @Override
    public boolean write() {
      return insert || delete || update;
    }

    @Override
    public ImmutableMap<String, Resource> resources() {
      Map<String, Resource> builder = new HashMap<>();
      String fullName = table.fullName();
      builder.put(fullName, new Resource(fullName, write()));
      if (reverseForeignIndexes) {
        for (ReverseForeignKey foreignKey : table.reverseForeignKeys().values()) {
          String refTableName = foreignKey.getRefTable().fullName();
          builder.put(refTableName, new Resource(refTableName, /* write= */false));
        }
      }
      return ImmutableMap.copyOf(builder);
    }

    @Override
    public DBObject getObject() {
      return table;
    }
  }

  private final ImmutableMap<String, MetaResource> objects;

  private MetaResources(ImmutableMap<String, MetaResource> objects) {
    this.objects = objects;
  }

  public Iterable<MetaResource> objects() {
    return objects.values();
  }

  public Iterable<TableMetaResource> tables() {
    return objects.values().stream().filter(o -> o instanceof TableMetaResource).map(o -> (TableMetaResource)o).collect(Collectors.toList());
  }

  public MetaResources merge(MetaResources other) {
    return new MetaResources(Util.merge(this.objects, other.objects, MetaResource::merge));
  }

  public static MetaResources readTable(MaterializedTable tableMeta) {
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

  public static MetaResources readView(TopLevelView view) {
    return new MetaResources(ImmutableMap.of(
        view.fullName(),
        new ViewMetaResource(
            view,
            /* meta= */false)));
  }

  public static MetaResources viewMeta(TopLevelView view) {
    return new MetaResources(ImmutableMap.of(
        view.fullName(),
        new ViewMetaResource(
            view,
            /* meta= */true)));
  }

  public static MetaResources readObject(DBObject object) {
    if (object instanceof MaterializedTable) {
      return readTable((MaterializedTable) object);
    } else if (object instanceof TopLevelView) {
      return readView((TopLevelView) object);
    } else {
      throw new IllegalArgumentException(
          String.format("Object '%s' has to be table or top level view.", object.fullName()));
    }
  }

  public static MetaResources empty() {
    return new MetaResources(ImmutableMap.of());
  }

  public Iterable<Resource> lockResources() {
    ImmutableMap<String, MetaResource> tables = Util.filter(objects, r -> r instanceof TableMetaResource);
    return Util.merge(
        Util.mapValues(tables, MetaResource::resources).values(),
        Resource::merge).values();
  }
}
