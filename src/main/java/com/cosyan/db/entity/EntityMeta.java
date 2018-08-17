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
package com.cosyan.db.entity;

import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.Method;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class EntityMeta extends Result {

  @Data
  public static class Entity {
    private final String name;
    private final ImmutableList<Field> fields;
    private final ImmutableList<ForeignKey> foreignKeys;
    private final ImmutableList<ReverseForeignKey> reverseForeignKeys;
    private final boolean insert;
    private final boolean delete;
    private final boolean update;

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("name", name);
      obj.put("insert", insert);
      obj.put("delete", delete);
      obj.put("update", update);
      obj.put("fields", fields.stream().map(f -> f.toJSON()).collect(Collectors.toList()));
      obj.put("foreignKeys", foreignKeys.stream().map(f -> f.toJSON()).collect(Collectors.toList()));
      obj.put("reverseForeignKeys", reverseForeignKeys.stream().map(f -> f.toJSON()).collect(Collectors.toList()));
      return obj;
    }
  }

  @Data
  public static class Field {
    private final String name;
    private final DataType<?> type;
    private final boolean searchField;

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("name", name);
      obj.put("type", type.toJSON());
      obj.put("search", searchField);
      return obj;
    }
  }

  @Data
  public static class ForeignKey {
    private final String name;
    private final DataType<?> type;
    private final String column;
    private final String refTable;

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("name", name);
      obj.put("type", type.toJSON());
      obj.put("column", column);
      obj.put("refTable", refTable);
      return obj;
    }
  }

  @Data
  public static class ReverseForeignKey {
    private final String name;
    private final DataType<?> type;
    private final String refColumn;
    private final String refTable;

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("name", name);
      obj.put("type", type.toJSON());
      obj.put("refColumn", refColumn);
      obj.put("refTable", refTable);
      return obj;
    }
  }

  private final ImmutableList<Entity> entities;

  public EntityMeta(MetaRepo metaRepo, AuthToken authToken) {
    super(true);
    ImmutableList<MaterializedTable> tables = metaRepo.getTables(authToken)
        .values()
        .stream()
        .filter(t -> t.pkColumn().map(c -> c.getType() == DataTypes.IDType).orElse(false))
        .collect(ImmutableList.toImmutableList());
    ImmutableList.Builder<Entity> entities = ImmutableList.builder();
    for (MaterializedTable table : tables) {
      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      for (BasicColumn column : table.columns().values()) {
        // TODO: search field based on stats
        if (!table.isColumnForeignKey(column.getName())) {
          fields.add(new Field(column.getName(), column.getType(), true));
        }
      }
      ImmutableList.Builder<ForeignKey> foreignKeys = ImmutableList.builder();
      for (com.cosyan.db.model.Keys.ForeignKey foreignKey : table.foreignKeys().values()) {
        foreignKeys.add(new ForeignKey(
            foreignKey.getName(),
            foreignKey.getColumn().getType(),
            foreignKey.getColumn().getName(),
            foreignKey.getRefTable().tableName()));
      }
      ImmutableList.Builder<ReverseForeignKey> reverseForeignKeys = ImmutableList.builder();
      for (com.cosyan.db.model.Keys.ReverseForeignKey reverseForeignKey : table.reverseForeignKeys().values()) {
        reverseForeignKeys.add(new ReverseForeignKey(
            reverseForeignKey.getName(),
            reverseForeignKey.getColumn().getType(),
            reverseForeignKey.getRefColumn().getName(),
            reverseForeignKey.getRefTable().tableName()));
      }
      entities.add(new Entity(
          table.tableName(),
          fields.build(),
          foreignKeys.build(),
          reverseForeignKeys.build(),
          metaRepo.hasAccess(table, authToken, Method.INSERT),
          metaRepo.hasAccess(table, authToken, Method.DELETE),
          metaRepo.hasAccess(table, authToken, Method.UPDATE)));
    }
    this.entities = entities.build();
  }

  @Override
  public JSONObject toJSON() {
    JSONObject obj = new JSONObject();
    JSONArray entitiesObj = new JSONArray();
    for (Entity entity : entities) {
      entitiesObj.put(entity.toJSON());
    }
    obj.put("entities", entitiesObj);
    return obj;
  }
}
