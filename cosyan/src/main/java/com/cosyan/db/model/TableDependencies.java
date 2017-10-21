package com.cosyan.db.model;

import com.cosyan.db.model.Keys.ForeignKey;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class TableDependencies {

  @Data
  public static class Dependency {
    private final ForeignKey foreignKey;
    private final TableDependencies deps;
  }
  
  private final ImmutableMap<String, Dependency> deps;
  
}
