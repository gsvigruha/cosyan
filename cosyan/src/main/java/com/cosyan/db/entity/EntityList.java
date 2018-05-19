package com.cosyan.db.entity;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.model.BasicColumn;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class EntityList extends Result {

  @Data
  public static class Header {
    private final String name;
    private final String type;
  }

  private final String entityType;
  private final ImmutableList<Header> header;
  private final ImmutableList<Object[]> valuess;

  public EntityList(String entityType, ImmutableList<BasicColumn> columns, ImmutableList<Object[]> valuess) {
    super(true);
    this.entityType = entityType;
    ImmutableList.Builder<Header> header = ImmutableList.builder();
    for (int i = 0; i < columns.size(); i++) {
      BasicColumn column = columns.get(i);
      header.add(new Header(column.getName(), column.getType().getName()));
    }
    this.header = header.build();
    this.valuess = valuess;
  }

  @Override
  public JSONObject toJSON() {
    JSONObject obj = new JSONObject();
    obj.put("type", entityType);
    JSONArray list = new JSONArray();
    for (Header h : header) {
      JSONObject fieldObj = new JSONObject();
      fieldObj.put("name", h.getName());
      fieldObj.put("type", h.getType());
      list.put(fieldObj);
    }
    obj.put("header", list);
    JSONArray valuessObj = new JSONArray();
    for (Object[] values : valuess) {
      JSONArray valuesObj = new JSONArray();
      for (Object value : values) {
        valuesObj.put(QueryResult.prettyPrint(value));
      }
      valuessObj.put(valuesObj);
    }
    obj.put("values", valuessObj);
    return obj;
  }
}
