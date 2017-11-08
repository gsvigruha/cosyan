package com.cosyan.db.model;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Dependencies.ColumnReverseRuleDependencies;
import com.cosyan.db.model.Dependencies.ReverseRuleDependency;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.Dependencies.TableDependency;
import com.cosyan.db.model.Dependencies.TransitiveTableDependency;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.References.MaterializedColumn;
import com.cosyan.db.model.References.SimpleMaterializedColumn;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class MaterializedTableMeta extends ExposedTableMeta {

  private final String tableName;
  private final List<BasicColumn> columns;
  private final Map<String, BooleanRule> rules;
  private Optional<PrimaryKey> primaryKey;
  private final Map<String, ForeignKey> foreignKeys;
  private final Map<String, ReverseForeignKey> reverseForeignKeys;
  private TableDependencies ruleDependencies;
  private ColumnReverseRuleDependencies reverseRuleDependencies;

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
    this.ruleDependencies = new TableDependencies();
    this.reverseRuleDependencies = new ColumnReverseRuleDependencies();
  }

  public ImmutableList<String> columnNames() {
    return columnsMap(columns).keySet().asList();
  }

  public ImmutableMap<String, BasicColumn> columns() {
    return columnsMap(columns);
  }

  public static ImmutableMap<String, BasicColumn> columnsMap(List<BasicColumn> columns) {
    // Need to keep iteration order;
    ImmutableMap.Builder<String, BasicColumn> builder = ImmutableMap.builder();
    for (BasicColumn column : columns) {
      if (!column.isDeleted()) {
        builder.put(column.getName(), column);
      }
    }
    return builder.build();
  }

  public ImmutableList<BasicColumn> allColumns() {
    return ImmutableList.copyOf(columns);
  }

  @Override
  public MaterializedColumn column(Ident ident) throws ModelException {
    if (!columns().containsKey(ident.getString())) {
      throw new ModelException(String.format("Column '%s' not found in table '%s'.", ident, tableName));
    }
    return new SimpleMaterializedColumn(
        this,
        columns().get(ident.getString()),
        columns().keySet().asList().indexOf(ident.getString()));
  }

  /*
   * public Column column(Ident ident) throws ModelException { if
   * (ident.isSimple()) { return simpleColumn(ident); } String[] parts =
   * ident.parts(); boolean startsWithTable = tableName.equals(ident.head());
   * MaterializedTableMeta referencingTable = this; ReferencedTableMeta
   * referencedTable = null; for (int i = startsWithTable ? 1 : 0; i <
   * parts.length - 1; i++) { String refName = parts[i]; if
   * (referencingTable.hasForeignKey(refName)) { ForeignKey foreignKey =
   * referencingTable.foreignKey(refName); referencingTable =
   * foreignKey.getRefTable(); referencedTable = new
   * ReferencedSimpleTableMeta(referencedTable, foreignKey); } else if
   * (referencingTable.hasReverseForeignKey(refName)) { ReverseForeignKey
   * reverseForeignKey = referencingTable.reverseForeignKey(refName);
   * referencingTable = reverseForeignKey.getRefTable(); referencedTable = new
   * ReferencedMultiTableMeta(referencedTable, reverseForeignKey); } else { throw
   * new ModelException(String.format( "Invalid reference '%s' in table '%s'.",
   * refName, referencingTable.tableName())); } } MaterializedColumn column =
   * referencingTable.column(new Ident(ident.last())); return new
   * SimpleReferencingColumn(referencedTable, column.getMeta(),
   * column.getIndex()); }
   */
  public boolean hasColumn(Ident ident) {
    try {
      return column(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  public String tableName() {
    return tableName;
  }

  public Map<String, BooleanRule> rules() {
    return Collections.unmodifiableMap(rules);
  }

  public Map<String, TableDependency> ruleDependencies() {
    return Collections.unmodifiableMap(ruleDependencies.getDeps());
  }

  public ColumnReverseRuleDependencies reverseRuleDependencies() {
    return reverseRuleDependencies;
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
    foreignKey.getRefTable().reverseForeignKeys.put(foreignKey.getRevName(), foreignKey.createReverse());
  }

  public void addColumn(BasicColumn basicColumn) throws ModelException {
    checkName(basicColumn.getName());
    columns.add(basicColumn);
  }

  public void addRule(BooleanRule rule) throws ModelException {
    checkName(rule.name());
    rules.put(rule.name(), rule);
    ruleDependencies.add(rule.getDeps());
    rule.getDeps().addAllReverseRuleDependencies(rule);
  }

  public void addReverseRuleDependency(
      BasicColumn column, Iterable<Ref> reverseForeignKeyChain, BooleanRule rule) {
    reverseRuleDependencies.addReverseRuleDependency(column, reverseForeignKeyChain, rule);
  }

  public ForeignKey foreignKey(String name) throws ModelException {
    if (!foreignKeys.containsKey(name)) {
      throw new ModelException(String.format("Invalid foreign key reference '%s' in table '%s'.", name, tableName));
    }
    return foreignKeys.get(name);
  }

  public boolean hasForeignKey(String name) {
    return foreignKeys.containsKey(name);
  }

  public ReverseForeignKey reverseForeignKey(String name) throws ModelException {
    if (!reverseForeignKeys.containsKey(name)) {
      throw new ModelException(
          String.format("Invalid reverse foreign key reference '%s' in table '%s'.", name, tableName));
    }
    return reverseForeignKeys.get(name);
  }

  public boolean hasReverseForeignKey(String name) {
    return reverseForeignKeys.containsKey(name);
  }

  public TableWriter writer(Resources resources) throws IOException {
    return resources.writer(new Ident(tableName));
  }

  public MetaResources ruleDependenciesReadResources() {
    return readResources(ruleDependencies.getDeps().values());
  }

  public MetaResources reverseRuleDependenciesReadResources() {
    MetaResources readResources = MetaResources.readTable(this);
    for (Map<String, ReverseRuleDependency> dependencies : reverseRuleDependencies.getColumnDeps().values()) {
      readResources = readResources.merge(readResources(dependencies.values()));
    }
    return readResources;
  }

  public static MetaResources readResources(Iterable<? extends TransitiveTableDependency> deps) {
    MetaResources readResources = MetaResources.empty();
    for (TransitiveTableDependency dependency : deps) {
      readResources = readResources.merge(readResources(dependency));
    }
    return readResources;
  }

  public static MetaResources readResources(TransitiveTableDependency dependency) {
    MetaResources readResources = MetaResources.readTable(dependency.table());
    for (TransitiveTableDependency childDep : dependency.childDeps()) {
      readResources = readResources.merge(readResources(childDep));
    }
    return readResources;
  }

  @Override
  protected SimpleMaterializedColumn getColumn(Ident ident) throws ModelException {
    if (!columns().containsKey(ident.getString())) {
      return null;
    }
    BasicColumn column = columns().get(ident.getString());
    return new SimpleMaterializedColumn(this, column, indexOf(columns().keySet(), ident));
  }

  @Override
  protected TableMeta getTable(Ident ident) throws ModelException {
    return References.ReferencedTableMeta.getTable(
        null,
        tableName,
        ident.getString(),
        foreignKeys,
        reverseForeignKeys);
  }

  @Override
  public SeekableTableReader reader(Resources resources) throws IOException {
    return resources.createReader(tableName);
  }

  @Override
  public MetaResources readResources() {
    return MetaResources.readTable(this);
  }
}
