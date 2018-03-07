package com.cosyan.db.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cosyan.db.model.DerivedTables.ReferencedDerivedTableMeta;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
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
    private final Set<String> columnDeps = new HashSet<>();

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
    private final Ref key;
    private final Map<String, ReverseRuleDependency> deps = new HashMap<>();
    private final Map<String, BooleanRule> rules = new HashMap<>();

    @Override
    public MaterializedTableMeta table() {
      return key.getRefTable();
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

    public TableDependencies() {
    }

    public TableDependencies(ReferencedDerivedTableMeta tableMeta,
        TableDependencies tableDependencies) {
      ReverseForeignKey reverseForeignKey = tableMeta.getReverseForeignKey();
      deps.put(reverseForeignKey.getName(), new TableDependency(reverseForeignKey));
      TableDependency tableDependency = deps.get(reverseForeignKey.getName());
      for (Map.Entry<String, TableDependency> other : tableDependencies.getDeps().entrySet()) {
        if (other.getValue().getForeignKey().equals(reverseForeignKey)) {
          deps.put(other.getKey(), other.getValue());
        } else {
          tableDependency.getDeps().put(other.getKey(), other.getValue());
        }
      }
    }

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
      tableDependency.columnDeps.add(column.getName());
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
      tableDependency.columnDeps.add(column.getName());
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
      for (String column : tableDependency.columnDeps) {
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

    public void addReverseRuleDependency(String column, Iterable<Ref> foreignKeyChain,
        BooleanRule rule) {
      if (!columnDeps.containsKey(column)) {
        columnDeps.put(column, new HashMap<>());
      }
      Map<String, ReverseRuleDependency> actDeps = columnDeps.get(column);
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