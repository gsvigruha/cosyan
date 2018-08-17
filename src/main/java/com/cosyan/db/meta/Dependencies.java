/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;

import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencedRefTableMeta;
import com.cosyan.db.model.References.ReferencedSimpleTableMeta;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.Rule.BooleanRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class Dependencies {

  public static interface TransitiveTableDependency {

    MaterializedTable table();

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
    public MaterializedTable table() {
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

    public void forAllReverseRuleDependencies(BooleanRule rule, boolean add) {
      for (TableDependency tableDependency : deps.values()) {
        LinkedList<Ref> reverseForeignKeyChain = new LinkedList<>();
        forAllReverseRuleDependencies(tableDependency, reverseForeignKeyChain, rule, add);
      }
    }

    private void forAllReverseRuleDependencies(
        TableDependency tableDependency,
        LinkedList<Ref> reverseForeignKeyChain,
        BooleanRule rule,
        boolean add) {
      Ref reverseRef = tableDependency.ref().getReverse();
      reverseForeignKeyChain.addFirst(reverseRef);
      if (add) {
        reverseRef.getTable().addReverseRuleDependency(reverseForeignKeyChain, rule);
      } else {
        reverseRef.getTable().removeReverseRuleDependency(reverseForeignKeyChain, rule);
      }
      for (TableDependency childDep : tableDependency.deps.values()) {
        LinkedList<Ref> newReverseForeignKeyChain = new LinkedList<>(reverseForeignKeyChain);
        forAllReverseRuleDependencies(childDep, newReverseForeignKeyChain, rule, add);
      }
    }

    public ImmutableMap<String, TableDependency> getDeps() {
      return ImmutableMap.copyOf(deps);
    }

    public Iterable<MaterializedTable> allTables() {
      ArrayList<MaterializedTable> tables = new ArrayList<>();
      allTables(tables, deps);
      return tables;
    }

    private void allTables(ArrayList<MaterializedTable> tables, Map<String, TableDependency> deps) {
      for (TableDependency dep : deps.values()) {
        tables.addAll(dep.deps.values().stream().map(d -> d.ref.getTable()).collect(Collectors.toList()));
        allTables(tables, dep.deps);
      }
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
    public MaterializedTable table() {
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
      BooleanRule existingRule = rules.get(rule.getName());
      if (existingRule == null) {
        rules.put(rule.getName(), rule);
      } else {
        assert existingRule == rule;
      }
    }

    public void removeRule(BooleanRule rule) {
      assert rule.getTable().tableName().equals(key.getRefTable().tableName());
      BooleanRule existingRule = rules.get(rule.getName());
      assert existingRule == rule;
      rules.remove(rule.getName());
    }
  }

  public static class ReverseRuleDependencies {
    private final Map<String, ReverseRuleDependency> deps = new HashMap<>();

    void addReverseRuleDependency(Iterable<Ref> foreignKeyChain, BooleanRule rule) {
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

    void removeReverseRuleDependency(Iterable<Ref> foreignKeyChain, BooleanRule rule) {
      Map<String, ReverseRuleDependency> actDeps = deps;
      ReverseRuleDependency reverseDep = null;
      for (Ref foreignKey : foreignKeyChain) {
        reverseDep = actDeps.get(foreignKey.getName());
        actDeps = reverseDep.getDeps();
      }
      reverseDep.removeRule(rule);
    }

    public ImmutableMap<String, ReverseRuleDependency> getDeps() {
      return ImmutableMap.copyOf(deps);
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