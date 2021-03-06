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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ViewDefinition;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.sql.SelectStatement.Select.TableColumns;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables.KeyValueAggrTableMeta;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.GroupByKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.References.GroupByFilterTableMeta;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.Rule.BooleanViewRule;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.google.common.collect.ImmutableList;

public abstract class View extends DBObject {

  private DerivedTableMeta tableMeta;
  private transient String expr;

  public View(String name, String owner) {
    super(name, owner);
  }

  public View(String name, MaterializedTable parentTable, String owner) {
    super(name, owner);
  }

  public DerivedTableMeta table() {
    return tableMeta;
  }

  public String expr() {
    return expr;
  }

  public void setTable(DerivedTableMeta tableMeta, String expr) {
    this.tableMeta = tableMeta;
    this.expr = expr;
  }

  private static TableColumns groupByTable(ViewDefinition ref, View view, SeekableTableMeta seekableTableMeta)
      throws ModelException, IOException {
    ImmutableList<Expression> groupBy = ref.getSelect().getGroupBy().get();
    GroupByKey groupByKey = new GroupByKey(
        "#" + Expression.hash(groupBy),
        view,
        seekableTableMeta.tableMeta(),
        Select.groupByColumns(seekableTableMeta, groupBy));
    GroupByFilterTableMeta selfAggrTableMeta = new GroupByFilterTableMeta(seekableTableMeta, groupByKey);
    ExposedTableMeta derivedTable;
    if (ref.getSelect().getWhere().isPresent()) {
      ColumnMeta whereColumn = ref.getSelect().getWhere().get().compileColumn(seekableTableMeta);
      derivedTable = new FilteredTableMeta(selfAggrTableMeta, whereColumn);
    } else {
      derivedTable = selfAggrTableMeta;
    }
    KeyValueTableMeta intermediateTable = new KeyValueTableMeta(derivedTable, groupByKey.getColumns());
    KeyValueAggrTableMeta aggrTable = new KeyValueAggrTableMeta(intermediateTable);
    TableColumns columns = Select.tableColumns(aggrTable, ref.getSelect().getColumns());
    seekableTableMeta.tableMeta().registerIndex(groupByKey);
    return columns;
  }

  public static DerivedTableMeta createView(ViewDefinition ref, View view, TableProvider tableProvider, String owner)
      throws ModelException, IOException {
    ExposedTableMeta srcTableMeta = ref.getSelect().getTable().compile(tableProvider, owner);
    if (srcTableMeta instanceof SeekableTableMeta) {
      SeekableTableMeta seekableTableMeta = (SeekableTableMeta) srcTableMeta;
      if (ref.getSelect().getGroupBy().isPresent()) {
        TableColumns columns = groupByTable(ref, view, seekableTableMeta);
        return new DerivedTableMeta(columns.getTable(), columns.getColumns());
      } else {
        TableColumns columns = Select.tableColumns(srcTableMeta, ref.getSelect().getColumns());
        return new DerivedTableMeta(srcTableMeta, columns.getColumns());
      }
    } else {
      throw new ModelException(String.format("Unsupported table '%s' for view.", ref.getSelect().getTable().print()),
          ref.getName());
    }
  }

  public abstract DBObject dbObject();

  @Override
  protected void addReverseRuleDependency(Iterable<Ref> reverseForeignKeyChain, Rule rule) {
  }

  @Override
  protected void removeReverseRuleDependency(Iterable<Ref> reverseForeignKeyChain, Rule rule) {
  }

  public static class TopLevelView extends View {

    private final Map<String, BooleanViewRule> rules;
    private final int index;

    public TopLevelView(String name, String owner, int index) {
      super(name, owner);
      this.rules = new HashMap<>();
      this.index = index;
    }

    public Map<String, BooleanViewRule> rules() {
      return Collections.unmodifiableMap(rules);
    }

    public int index() {
      return index;
    }

    public void checkName(Ident ident) throws ModelException {
      String name = ident.getString();
      if (rules.containsKey(name)) {
        throw new ModelException(String.format("Duplicate name in '%s': '%s'.", name(), name), ident);
      }
    }

    public BooleanViewRule createRule(RuleDefinition ruleDefinition) throws ModelException {
      checkName(ruleDefinition.getName());
      return ruleDefinition.compile(this);
    }

    public void addRule(BooleanViewRule rule) {
      rules.put(rule.getName(), rule);
    }

    @Override
    public DBObject dbObject() {
      return this;
    }
  }

  public static class SubView extends View {

    private final MaterializedTable parentTable;

    public SubView(String name, MaterializedTable parentTable, String owner) {
      super(name, parentTable, owner);
      this.parentTable = parentTable;
    }

    @Override
    public DBObject dbObject() {
      return parentTable;
    }
  }
}
