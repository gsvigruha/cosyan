package com.cosyan.db.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

import com.cosyan.db.model.DerivedTables.ReferencedDerivedTableMeta;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencedSimpleTableMeta;
import com.cosyan.db.model.Rule.BooleanRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import lombok.Data;

public class Dependencies {

  public static interface TransitiveTableDependency {

    MaterializedTableMeta table();

    Iterable<? extends TransitiveTableDependency> childDeps();

  }

  @Data
  public static class TableDependency implements TransitiveTableDependency {
    private final Ref ref;
    private final Map<String, TableDependency> deps = new HashMap<>();

    public void merge(TableDependency other) {
      assert this.ref == other.ref;
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
      return ref.getRefTable();
    }

    @Override
    public Iterable<? extends TransitiveTableDependency> childDeps() {
      return deps.values();
    }
  }

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
        if (other.getValue().getRef().equals(reverseForeignKey)) {
          deps.put(other.getKey(), other.getValue());
        } else {
          tableDependency.getDeps().put(other.getKey(), other.getValue());
        }
      }
    }

    public void addTableDependency(ReferencedSimpleTableMeta table) {
      Map<String, TableDependency> actDeps = deps;
      for (Ref foreignKey : table.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        actDeps = actDeps.get(foreignKey.getName()).getDeps();
      }
    }

    public void addTableDependency(ReferencedMultiTableMeta tableMeta) {
      Map<String, TableDependency> actDeps = deps;
      for (Ref foreignKey : tableMeta.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        actDeps = actDeps.get(foreignKey.getName()).getDeps();
      }
    }

    public TableDependencies addToThis(TableDependencies other) {
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
      Ref reverseRef = tableDependency.getRef().getReverse();
      reverseForeignKeyChain.addFirst(reverseRef);
      reverseRef.getTable().addReverseRuleDependency(reverseForeignKeyChain, rule);
      for (TableDependency childDep : tableDependency.getDeps().values()) {
        LinkedList<Ref> newReverseForeignKeyChain = new LinkedList<>(reverseForeignKeyChain);
        addAllReverseRuleDependencies(childDep, newReverseForeignKeyChain, rule);
      }
    }

    public ImmutableMap<String, TableDependency> getDeps() {
      return ImmutableMap.copyOf(deps);
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

  public static class ReverseRuleDependencies {
    private final Map<String, ReverseRuleDependency> deps = new HashMap<>();

    public void addReverseRuleDependency(Iterable<Ref> foreignKeyChain, BooleanRule rule) {
      Map<String, ReverseRuleDependency> actDeps = deps;
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

    public Map<String, ReverseRuleDependency> getDeps() {
      return deps;
    }
  }
}