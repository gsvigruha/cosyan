package com.cosyan.db.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.References.RefTableMeta;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencedRefTableMeta;
import com.cosyan.db.model.References.ReferencedSimpleTableMeta;
import com.cosyan.db.model.Rule.BooleanRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class Dependencies {

  public static interface TransitiveTableDependency {

    MaterializedTableMeta table();

    Iterable<? extends TransitiveTableDependency> childDeps();

  }

  public static class TableDependency implements TransitiveTableDependency {
    private final Ref ref;
    private final Map<String, TableDependency> deps = new HashMap<>();

    public TableDependency(Ref ref) {
      this.ref = ref;
    }

    public void merge(TableDependency other) {
      assert this.ref == other.ref;
      for (Map.Entry<String, TableDependency> entry : other.deps.entrySet()) {
        if (this.deps.containsKey(entry.getKey())) {
          this.deps.get(entry.getKey()).merge(entry.getValue());
        } else {
          assert ref.getRefTable() == entry.getValue().ref.getTable();
          this.deps.put(entry.getKey(), entry.getValue());
        }
      }
    }

    public Ref ref() {
      return ref;
    }

    public TableDependency dep(String keyName) {
      return deps.get(keyName);
    }

    public int size() {
      return deps.size();
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

    private final Map<String, TableDependency> deps;

    public TableDependencies() {
      deps = new HashMap<>();
    }

    public TableDependencies(RefTableMeta tableMeta,
        TableDependencies tableDependencies) {
      deps = new HashMap<>();
      ReverseForeignKey reverseForeignKey = tableMeta.getReverseForeignKey();
      deps.put(reverseForeignKey.getName(), new TableDependency(reverseForeignKey));
      TableDependency tableDependency = deps.get(reverseForeignKey.getName());
      for (Map.Entry<String, TableDependency> other : tableDependencies.getDeps().entrySet()) {
        if (other.getValue().ref().equals(reverseForeignKey)) {
          deps.put(other.getKey(), other.getValue());
        } else {
          tableDependency.deps.put(other.getKey(), other.getValue());
        }
      }
    }

    public TableDependencies(ReferencedRefTableMeta table, TableDependencies tableDependencies) {
      deps = new HashMap<>();
      Map<String, TableDependency> actDeps = deps;
      for (Ref foreignKey : table.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        actDeps = actDeps.get(foreignKey.getName()).deps;
      }
      actDeps.putAll(tableDependencies.getDeps());
    }

    public void addTableDependency(ReferencedSimpleTableMeta table) {
      Map<String, TableDependency> actDeps = deps;
      for (Ref foreignKey : table.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        actDeps = actDeps.get(foreignKey.getName()).deps;
      }
    }

    public void addTableDependency(ReferencedMultiTableMeta tableMeta) {
      Map<String, TableDependency> actDeps = deps;
      for (Ref foreignKey : tableMeta.foreignKeyChain()) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new TableDependency(foreignKey));
        }
        actDeps = actDeps.get(foreignKey.getName()).deps;
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
      Ref reverseRef = tableDependency.ref().getReverse();
      reverseForeignKeyChain.addFirst(reverseRef);
      reverseRef.getTable().addReverseRuleDependency(reverseForeignKeyChain, rule);
      for (TableDependency childDep : tableDependency.deps.values()) {
        LinkedList<Ref> newReverseForeignKeyChain = new LinkedList<>(reverseForeignKeyChain);
        addAllReverseRuleDependencies(childDep, newReverseForeignKeyChain, rule);
      }
    }

    public ImmutableMap<String, TableDependency> getDeps() {
      return ImmutableMap.copyOf(deps);
    }
  }

  public static class ReverseRuleDependency implements TransitiveTableDependency {
    private final Ref key;
    private final Map<String, ReverseRuleDependency> deps = new HashMap<>();
    private final Map<String, BooleanRule> rules = new HashMap<>();

    public ReverseRuleDependency(Ref key) {
      this.key = key;
    }

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

    public BooleanRule rule(String name) {
      return rules.get(name);
    }

    public Collection<BooleanRule> rules() {
      return rules.values();
    }

    public Map<String, ReverseRuleDependency> getDeps() {
      return deps;
    }

    public Ref getKey() {
      return key;
    }

    public void addRule(BooleanRule rule) {
      assert rule.getTable().tableName().equals(key.getRefTable().tableName());
      rules.put(rule.getName(), rule);
    }
  }

  public static class ReverseRuleDependencies {
    private final Map<String, ReverseRuleDependency> deps = new HashMap<>();

    public void addReverseRuleDependency(Iterable<Ref> foreignKeyChain, BooleanRule rule) {
      Map<String, ReverseRuleDependency> actDeps = deps;
      ReverseRuleDependency reverseDep = null;
      for (Ref foreignKey : foreignKeyChain) {
        if (!actDeps.containsKey(foreignKey.getName())) {
          actDeps.put(foreignKey.getName(), new ReverseRuleDependency(foreignKey));
        }
        reverseDep = actDeps.get(foreignKey.getName());
        actDeps = reverseDep.getDeps();
      }
      reverseDep.addRule(rule);
    }

    public Map<String, ReverseRuleDependency> getDeps() {
      return deps;
    }

    public Iterable<Rule> allRules() {
      ArrayList<Rule> rules = new ArrayList<>();
      allRules(rules, deps);
      return rules;
    }

    private void allRules(ArrayList<Rule> rules, Map<String, ReverseRuleDependency> deps) {
      for (ReverseRuleDependency dep : deps.values()) {
        rules.addAll(dep.rules.values());
        allRules(rules, dep.deps);
      }
    }
  }
}