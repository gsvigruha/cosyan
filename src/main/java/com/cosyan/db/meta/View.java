package com.cosyan.db.meta;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.TableDefinition.ViewDefinition;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.sql.SelectStatement.Select.TableColumns;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables.KeyValueAggrTableMeta;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Keys.GroupByKey;
import com.cosyan.db.model.References.AggRefTableMeta;
import com.cosyan.db.model.References.FlatRefTableMeta;
import com.cosyan.db.model.References.GroupByFilterTableMeta;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.google.common.collect.ImmutableList;

public class View {

  private final String name;
  private final String owner;
  private final ExposedTableMeta tableMeta;
  private final Map<String, BooleanRule> rules;

  public View(String name, String owner, ExposedTableMeta tableMeta) {
    this.name = name;
    this.owner = owner;
    this.tableMeta = tableMeta;
    this.rules = new HashMap<>();
  }

  public String owner() {
    return owner;
  }

  public String name() {
    return name;
  }

  public ExposedTableMeta table() {
    return tableMeta;
  }

  public Map<String, BooleanRule> rules() {
    return Collections.unmodifiableMap(rules);
  }

  private static TableColumns groupByTable(ViewDefinition ref, SeekableTableMeta seekableTableMeta) throws ModelException, IOException {
    ImmutableList<Expression> groupBy = ref.getSelect().getGroupBy().get();
    GroupByKey groupByKey = new GroupByKey(
        "#" + groupBy.stream().map(c -> c.print()).collect(Collectors.joining("#")),
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

  public static TableMeta createRefView(ViewDefinition ref, TableProvider tableProvider, String owner)
      throws ModelException, IOException {
    ExposedTableMeta srcTableMeta = ref.getSelect().getTable().compile(tableProvider, owner);
    if (srcTableMeta instanceof SeekableTableMeta) {
      SeekableTableMeta seekableTableMeta = (SeekableTableMeta) srcTableMeta;
      if (ref.getSelect().getGroupBy().isPresent()) {
        TableColumns columns = groupByTable(ref, seekableTableMeta);
        return new AggRefTableMeta(columns.getTable(), columns.getColumns());
      } else {
        TableColumns columns = Select.tableColumns(srcTableMeta, ref.getSelect().getColumns());
        return new FlatRefTableMeta(srcTableMeta, columns.getColumns());
      }
    } else {
      throw new ModelException(String.format("Unsupported table '%s' for view.", ref.getSelect().getTable().print()),
          ref.getName());
    }
  }

  public static ExposedTableMeta createView(ViewDefinition ref, TableProvider tableProvider, String owner)
      throws ModelException, IOException {
    ExposedTableMeta srcTableMeta = ref.getSelect().getTable().compile(tableProvider, owner);
    if (srcTableMeta instanceof SeekableTableMeta) {
      SeekableTableMeta seekableTableMeta = (SeekableTableMeta) srcTableMeta;
      if (ref.getSelect().getGroupBy().isPresent()) {
        TableColumns columns = groupByTable(ref, seekableTableMeta);
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
}
