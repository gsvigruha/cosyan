package com.cosyan.db.model;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class MaterializedTableMeta extends ExposedTableMeta {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MaterializedColumn extends Column {

    private final ImmutableList<ForeignKey> foreignKeyChain;

    public MaterializedColumn(BasicColumn column, int index, ImmutableList<ForeignKey> foreignKeyChain) {
      super(column, index);
      this.foreignKeyChain = foreignKeyChain;
    }

    @Override
    public BasicColumn getMeta() {
      return (BasicColumn) super.getMeta();
    }

    @Override
    public boolean usesSourceValues() {
      return foreignKeyChain.isEmpty();
    }

    @Override
    public String tableIdent() {
      return foreignKeyChain.stream().map(foreignKey -> foreignKey.getName()).collect(Collectors.joining("."));
    }
  }

  private final String tableName;
  private final List<BasicColumn> columns;
  private final Map<String, BooleanRule> rules;
  private Optional<PrimaryKey> primaryKey;
  private final Map<String, ForeignKey> foreignKeys;
  private final Map<String, ReverseForeignKey> reverseForeignKeys;

  public MaterializedTableMeta(
      String tableName,
      Iterable<BasicColumn> columns,
      Optional<PrimaryKey> primaryKey) {
    this.tableName = tableName;
    this.columns = Lists.newArrayList(columns);
    this.primaryKey = primaryKey;
    this.rules = new HashMap<>();
    this.foreignKeys = new HashMap<>();
    this.reverseForeignKeys = new HashMap<>();
  }

  @Override
  public ImmutableList<String> columnNames() {
    return columnsMap(columns).keySet().asList();
  }

  public ImmutableMap<String, BasicColumn> columns() {
    return columnsMap(columns);
  }

  public static ImmutableMap<String, BasicColumn> columnsMap(List<BasicColumn> columns) {
    return ImmutableMap.copyOf(columns
        .stream()
        .filter(column -> !column.isDeleted())
        .collect(Collectors.toMap(BasicColumn::getName, column -> column)));
  }

  public ImmutableList<BasicColumn> allColumns() {
    return ImmutableList.copyOf(columns);
  }

  @Override
  public SeekableTableReader reader(Resources resources) throws IOException {
    return resources.reader(new Ident(tableName));
  }

  @Override
  public MaterializedColumn getColumn(Ident ident) throws ModelException {
    if (ident.isSimple()) {
      if (!columns().containsKey(ident.getString())) {
        return null;
      }
      return new MaterializedColumn(
          columns().get(ident.getString()),
          indexOf(columns().keySet(), ident),
          ImmutableList.of());
    }
    String[] parts = ident.parts();
    boolean startsWithTable = tableName.equals(ident.head());
    ImmutableList.Builder<ForeignKey> foreignKeyChain = ImmutableList.builder();
    MaterializedTableMeta refTable = this;
    for (int i = startsWithTable ? 1 : 0; i < parts.length - 1; i++) {
      ForeignKey foreignKey = refTable.foreignKey(parts[i]);
      refTable = foreignKey.getRefTable();
      foreignKeyChain.add(foreignKey);
    }
    MaterializedColumn column = refTable.column(ident.tail());
    return new MaterializedColumn(column.getMeta(), column.getIndex(), foreignKeyChain.build());
  }

  @Override
  public MaterializedColumn column(Ident ident) throws ModelException {
    return (MaterializedColumn) super.column(ident);
  }

  @Override
  public MetaResources readResources() {
    return MetaResources.readTable(this);
  }

  public String tableName() {
    return tableName;
  }

  public Map<String, BooleanRule> rules() {
    return Collections.unmodifiableMap(rules);
  }

  public Optional<PrimaryKey> primaryKey() {
    return primaryKey;
  }

  public Map<String, ForeignKey> foreignKeys() {
    return Collections.unmodifiableMap(foreignKeys);
  }

  public Map<String, ReverseForeignKey> reverseForeignKeys() {
    return Collections.unmodifiableMap(reverseForeignKeys);
  }

  private void checkName(String name) throws ModelException {
    if (columnNames().contains(name) || foreignKeys.containsKey(name) || reverseForeignKeys.containsKey(name)) {
      throw new ModelException(
          String.format("Duplicate column, foreign key or reversed foreign key name in '%s': '%s'.", tableName, name));
    }
  }

  public void addForeignKey(ForeignKey foreignKey) throws ModelException {
    checkName(foreignKey.getName());
    foreignKey.getRefTable().checkName(foreignKey.getName());
    foreignKeys.put(foreignKey.getName(), foreignKey);
    foreignKey.getRefTable().reverseForeignKeys.put(foreignKey.getName(), foreignKey.reverse(this));
  }

  public void addColumn(BasicColumn basicColumn) throws ModelException {
    checkName(basicColumn.getName());
    columns.add(basicColumn);
  }

  public void addRule(BooleanRule rule) throws ModelException {
    checkName(rule.name());
    rules.put(rule.name(), rule);
  }

  public ForeignKey foreignKey(String name) throws ModelException {
    if (!foreignKeys.containsKey(name)) {
      throw new ModelException(String.format("Invalid foreign key reference '%s' in table '%s'.", name, tableName));
    }
    return foreignKeys.get(name);
  }
}
