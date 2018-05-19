package com.cosyan.db.entity;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.model.BasicColumn;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class EntityMeta extends Result {

  @Data
  public static class Entity {
    private final String name;
    private final ImmutableList<Field> fields;

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("name", name);
      JSONArray fieldsObj = new JSONArray();
      for (Field field : fields) {
        fieldsObj.put(field.toJSON());
      }
      obj.put("fields", fieldsObj);
      return obj;
    }
  }

  @Data
  public static class Field {
    private final String name;
    private final String type;
    private final boolean searchField;

    public JSONObject toJSON() {
      JSONObject obj = new JSONObject();
      obj.put("name", name);
      obj.put("type", type);
      obj.put("search", searchField);
      return obj;
    }
  }

  private final ImmutableList<Entity> entities;

  public EntityMeta(ImmutableList<MaterializedTable> tables) {
    super(true);
    ImmutableList.Builder<Entity> entities = ImmutableList.builder();
    for (MaterializedTable table : tables) {
      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      for (BasicColumn column : table.columns().values()) {
        // TODO: search field based on stats
        fields.add(new Field(column.getName(), column.getType().getName(), true));
      }
      entities.add(new Entity(table.tableName(), fields.build()));
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
