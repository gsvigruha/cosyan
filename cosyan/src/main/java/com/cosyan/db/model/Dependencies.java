package com.cosyan.db.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencedSimpleTableMeta;
import com.cosyan.db.model.Rule.BooleanRule;
import com.google.common.collect.Iterables;

import lombok.Data;

public class Dependencies {

  public static interface TransitiveTableDependency {

    MaterializedTableMeta table();

    Iterable<? extends TransitiveTableDependency> childDeps();

  }

  @Data
  public static class TableDependency implements TransitiveTableDependency {
    private final Ref foreignKey;
    private final Map<String, TableDependency> deps = new HashMap<>();
    private final Map<String, BasicColumn> columnDeps = new HashMap<>();

    public void merge(TableDependency other) {
      assert this.foreignKey == other.foreignKey;
      for (Map.Entry<String, TableDependency> entry : other.deps.entrySet()) {
        if (this.deps.containsKey(entry.getKey())) {
          this.deps.get(entry.getKey()).merge(entry.getValue());
        } else {
          this.deps.put(entry.getKey(), entry.getValue());
        }
      }
    }

    @Override
    public MaterializedTableMeta table() {
      return foreignKey.getRefTable();
    }

    @Override
    public Iterable<? extends TransitiveTableDependency> childDeps() {
      return deps.values();
    }
  }

  @Data
  public static class ReverseRuleDependency implements TransitiveTableDependency {
    private final Ref foreignKey;
    private final Map<String, ReverseRuleDependency> deps = new HashMap<>();
    private final Map<String, BooleanRule> rules = new HashMap<>();

    @Override
    public MaterializedTableMeta table() {
      return foreignKey.getRefTable();
    }

    @Override
    public Iterable<? extends TransitiveTableDependency> childDeps() {
      return Iterables.concat(
          deps.values(),
          rules.values().stream().flatMap(rule -> rule.getDeps().getDeps().values().stream())
              .collect(Collectors.toList()));
    }
  }

  @Data
  public static class TableDependencies {

    private final Map<String, TableDependency> deps = new HashMap<>();

    public void addTableDependency(ReferencedSimpleTableMeta table, BasicColumn column) {
      Map<String, TableDependency> actDeps = deps;
      TableDependency tableDependency = null;
      for (Ref foreignKey : table.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        tableDependency = actDeps.get(foreignKey.getName());
        actDeps = tableDependency.getDeps();
      }
      tableDependency.columnDeps.put(column.getName(), column);
    }

    public void addTableDependency(ReferencedMultiTableMeta tableMeta, BasicColumn column) {
      Map<String, TableDependency> actDeps = deps;
      TableDependency tableDependency = null;
      for (Ref foreignKey : tableMeta.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        tableDependency = actDeps.get(foreignKey.getName());
        actDeps = tableDependency.getDeps();
      }
      tableDependency.columnDeps.put(column.getName(), column);
    }

    public TableDependencies add(TableDependencies other) {
      for (Map.Entry<String, TableDependency> entry : other.deps.entrySet()) {
        if (this.deps.containsKey(entry.getKey())) {
          this.deps.get(entry.getKey()).merge(entry.getValue());
        } else {
          this.deps.put(entry.getKey(), entry.getValue());
        }
      }
      return this;
    }

    public void addAllReverseRuleDependencies(BooleanRule rule) {
      for (TableDependency tableDependency : deps.values()) {
        LinkedList<Ref> reverseForeignKeyChain = new LinkedList<>();
        addAllReverseRuleDependencies(tableDependency, reverseForeignKeyChain, rule);
      }
    }

    private void addAllReverseRuleDependencies(
        TableDependency tableDependency,
        LinkedList<Ref> reverseForeignKeyChain,
        BooleanRule rule) {
      Ref reverseForeignKey = tableDependency.getForeignKey().getReverse();
      reverseForeignKeyChain.addFirst(reverseForeignKey);
      for (BasicColumn column : tableDependency.columnDeps.values()) {
        reverseForeignKey.getTable().addReverseRuleDependency(column, reverseForeignKeyChain, rule);
      }
      for (TableDependency childDep : tableDependency.getDeps().values()) {
        LinkedList<Ref> newReverseForeignKeyChain = new LinkedList<>(reverseForeignKeyChain);
        addAllReverseRuleDependencies(childDep, newReverseForeignKeyChain, rule);
      }
    }
  }

  @Data
  public static class ColumnReverseRuleDependencies {
    private final Map<String, Map<String, ReverseRuleDependency>> columnDeps = new HashMap<>();

    public void addReverseRuleDependency(BasicColumn column, Iterable<Ref> foreignKeyChain,
        BooleanRule rule) {
      if (!columnDeps.containsKey(column.getName())) {
        columnDeps.put(column.getName(), new HashMap<>());
      }
      Map<String, ReverseRuleDependency> actDeps = columnDeps.get(column.getName());
      Map<String, BooleanRule> rules = null;
      for (Ref foreignKey : foreignKeyChain) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new ReverseRuleDependency(foreignKey));
        }
        ReverseRuleDependency reverseDep = actDeps.get(foreignKey.getName());
        actDeps = reverseDep.getDeps();
        rules = reverseDep.getRules();
      }
      rules.put(rule.getName(), rule);
    }
    
    public Collection<ReverseRuleDependency> allReverseRuleDepenencies() {
      return columnDeps.values().stream().flatMap(r -> r.values().stream()).collect(Collectors.toList());
    }
  }
}