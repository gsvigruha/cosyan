package com.cosyan.db.model;

import com.google.common.collect.ImmutableMap;

import lombok.Data;

public interface CompiledObject {

  @Data
  public static class ColumnList implements CompiledObject {
    private final ImmutableMap<String, ColumnMeta> columns;
  }
}
