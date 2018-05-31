package com.cosyan.db.entity;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.model.BasicColumn;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class Entity extends Result {

  @Data
  public static class Field {
    private final String name;
    private final String type;
    private final Object value;
  }

  private final String entityType;
  private final String pkColumn;
  private final ImmutableList<Field> fields;

  public Entity(String entityType, String pkColumn, ImmutableList<BasicColumn> header, Object[] values) {
    super(true);
    this.entityType = entityType;
    this.pkColumn = pkColumn;
    ImmutableList.Builder<Field> fields = ImmutableList.builder();
    for (int i = 0; i < header.size(); i++) {
      BasicColumn column = header.get(i);
      fields.add(new Field(column.getName(), column.getType().getName(), values[i]));
    }
    this.fields = fields.build();
  }

  @Override
  public JSONObject toJSON() {
    JSONObject obj = new JSONObject();
    obj.put("type", entityType);
    obj.put("pk", pkColumn);
    JSONArray list = new JSONArray();
    for (Field field : fields) {
      JSONObject fieldObj = new JSONObject();
      fieldObj.put("name", field.getName());
      fieldObj.put("type", field.getType());
      fieldObj.put("value", QueryResult.prettyPrint(field.getValue()));
      list.put(fieldObj);
    }
    obj.put("fields", list);
    return obj;
  }
}
