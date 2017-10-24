package com.cosyan.db.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class TableDependencies {

  @Data
  public static class Dependency {
    private final ForeignKey foreignKey;
    private final Map<String, Dependency> deps = new HashMap<>();
  }

  private final Map<String, Dependency> deps = new HashMap<>();
  private final List<AggrColumn> aggrColumns = new ArrayList<>();

  public void add(ImmutableList<ForeignKey> foreignKeyChain) {
    Map<String, Dependency> actDeps = deps;
    for (ForeignKey foreignKey : foreignKeyChain) {
      if (!actDeps.containsKey(foreignKey.getName())) {
        actDeps.put(foreignKey.getName(), new Dependency(foreignKey));
      }
      actDeps = actDeps.get(foreignKey.getName()).getDeps();
    }
  }

  public void addAggrColumn(AggrColumn aggrColumn) {
    aggrColumns.add(aggrColumn);
  }

  public int numAggrColumns() {
    return aggrColumns.size();
  }
  
  public ImmutableList<AggrColumn> aggrColumns() {
    return ImmutableList.copyOf(aggrColumns);
  }
}
