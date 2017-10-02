package com.cosyan.db.model;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import com.cosyan.db.conf.Config;
import com.cosyan.db.index.ByteMultiTrie.LongMultiIndex;
import com.cosyan.db.index.ByteMultiTrie.StringMultiIndex;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;
import com.cosyan.db.io.Serializer;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.TableIndex.LongTableIndex;
import com.cosyan.db.model.TableIndex.StringTableIndex;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.model.TableMultiIndex.LongTableMultiIndex;
import com.cosyan.db.model.TableMultiIndex.StringTableMultiIndex;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class MetaRepo {

  private final Config config;
  private final ConcurrentHashMap<String, MaterializedTableMeta> tables;
  private final ConcurrentHashMap<String, TableIndex> uniqueIndexes;
  private final ConcurrentHashMap<String, TableMultiIndex> multiIndexes;

  private final ConcurrentHashMap<String, SimpleFunction<?>> simpleFunctions;
  private final ConcurrentHashMap<String, AggrFunction> aggrFunctions;

  private final LockManager lockManager;

  public MetaRepo(Config config, LockManager lockManager) throws IOException, ModelException {
    this.config = config;
    this.lockManager = lockManager;
    this.tables = new ConcurrentHashMap<>();
    this.uniqueIndexes = new ConcurrentHashMap<>();
    this.multiIndexes = new ConcurrentHashMap<>();
    this.simpleFunctions = new ConcurrentHashMap<>();
    for (SimpleFunction<?> simpleFunction : BuiltinFunctions.SIMPLE) {
      simpleFunctions.put(simpleFunction.getIdent(), simpleFunction);
    }
    this.aggrFunctions = new ConcurrentHashMap<>();
    for (AggrFunction aggrFunction : BuiltinFunctions.AGGREGATIONS) {
      aggrFunctions.put(aggrFunction.getIdent(), aggrFunction);
    }
    Files.createDirectories(Paths.get(config.dataDir()));
    Files.createDirectories(Paths.get(config.metaDir()));
    Files.createDirectories(Paths.get(config.tableDir()));
    Files.createDirectories(Paths.get(config.indexDir()));
    // Load tables.
    ImmutableList<String> tableNames = ImmutableList.copyOf(Files.list(Paths.get(config.metaDir()))
        .map(path -> path.getFileName().toString())
        .filter(path -> !path.contains("#"))
        .iterator());
    for (String tableName : tableNames) {
      FileInputStream tableIn = new FileInputStream(config.metaDir() + File.separator + tableName);
      tables.put(tableName, Serializer.readTableMeta(tableName, tableIn, this));
      lockManager.registerLock(tableName);
    }
    // Load table references (foreign keys).
    for (String tableName : tableNames) {
      FileInputStream refIn = new FileInputStream(config.metaDir() + File.separator + tableName + "#ref");
      Serializer.readTableReferences(tables.get(tableName), refIn, this);
    }
    // Load indexes.
    for (String tableName : tableNames) {
      MaterializedTableMeta tableMeta = tables.get(tableName);
      for (BasicColumn column : tableMeta.columns().values()) {
        if (column.isIndexed()) {
          if (column.isUnique()) {
            registerUniqueIndex(tableMeta, column);
          } else {
            registerMultiIndex(tableMeta, column);
          }
        }
      }
    }
  }

  public MaterializedTableMeta table(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid table identifier '" + ident.getString() + "'.");
    }
    if (!tables.containsKey(ident.getString())) {
      throw new ModelException("Table '" + ident.getString() + "' does not exist.");
    }
    return tables.get(ident.getString());
  }

  private FileInputStream fileInputStream(MaterializedTableMeta table) throws ModelException {
    String path = config.tableDir() + File.separator + table.getTableName();
    try {
      return new FileInputStream(path);
    } catch (FileNotFoundException e) {
      throw new ModelException("Table file not found: " + path + ".");
    }
  }

  private RandomAccessFile randomAccessFile(MaterializedTableMeta table) throws ModelException {
    String path = config.tableDir() + File.separator + table.getTableName();
    try {
      return new RandomAccessFile(path, "rw");
    } catch (FileNotFoundException e) {
      throw new ModelException("Table file not found: " + path + ".");
    }
  }

  public ImmutableMap<String, TableIndex> collectUniqueIndexes(MaterializedTableMeta table) throws ModelException {
    ImmutableMap.Builder<String, TableIndex> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      String indexName = table.getTableName() + "." + column.getName();
      if (uniqueIndexes.containsKey(indexName)) {
        builder.put(column.getName(), uniqueIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  public ImmutableMultimap<String, TableIndex> collectForeignIndexes(MaterializedTableMeta table)
      throws ModelException {
    ImmutableMultimap.Builder<String, TableIndex> builder = ImmutableMultimap.builder();
    for (ForeignKey foreignKey : table.getForeignKeys().values()) {
      builder.put(
          foreignKey.getColumn().getName(),
          uniqueIndexes.get(foreignKey.getRefTable().getTableName() + "." + foreignKey.getRefColumn().getName()));
    }
    return builder.build();
  }

  public ImmutableMultimap<String, TableMultiIndex> collectReverseForeignIndexes(MaterializedTableMeta table)
      throws ModelException {
    ImmutableMultimap.Builder<String, TableMultiIndex> builder = ImmutableMultimap.builder();
    for (ForeignKey reverseForeignKey : table.getReverseForeignKeys().values()) {
      builder.put(
          reverseForeignKey.getColumn().getName(),
          multiIndexes
              .get(reverseForeignKey.getRefTable().getTableName() + "." + reverseForeignKey.getRefColumn().getName()));
    }
    return builder.build();
  }

  public ImmutableMap<String, TableMultiIndex> collectMultiIndexes(MaterializedTableMeta table) throws ModelException {
    ImmutableMap.Builder<String, TableMultiIndex> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      String indexName = table.getTableName() + "." + column.getName();
      if (multiIndexes.containsKey(indexName)) {
        builder.put(column.getName(), multiIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  public void registerTable(String tableName, MaterializedTableMeta tableMeta) throws IOException {
    File file = new File(config.tableDir() + File.separator + tableName);
    file.createNewFile();
    FileOutputStream tableOut = new FileOutputStream(config.metaDir() + File.separator + tableName);
    FileOutputStream referencesOut = new FileOutputStream(config.metaDir() + File.separator + tableName + "#ref");
    Serializer.writeTableMeta(tableMeta, tableOut, referencesOut);
    tableOut.close();
    referencesOut.close();
    tables.put(tableName, tableMeta);
    lockManager.registerLock(tableName);
  }

  public void registerUniqueIndex(MaterializedTableMeta table, BasicColumn column) throws ModelException, IOException {
    String indexName = table.getTableName() + "." + column.getName();
    String path = config.indexDir() + File.separator + indexName;
    assert !uniqueIndexes.containsKey(indexName);
    if (column.isUnique()) {
      if (column.getType() == DataTypes.StringType) {
        uniqueIndexes.put(indexName, new StringTableIndex(new StringIndex(path)));
      } else if (column.getType() == DataTypes.LongType) {
        uniqueIndexes.put(indexName, new LongTableIndex(new LongIndex(path)));
      } else {
        throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
            " and " + DataTypes.LongType + " types, not " + column.getType() + ".");
      }
    } else {
      throw new ModelException("Column " + column.getName() + " is not unique.");
    }
  }

  public void registerMultiIndex(MaterializedTableMeta table, BasicColumn column) throws ModelException, IOException {
    String indexName = table.getTableName() + "." + column.getName();
    String path = config.indexDir() + File.separator + indexName;
    assert !multiIndexes.containsKey(indexName);
    if (column.getType() == DataTypes.StringType) {
      multiIndexes.put(indexName, new StringTableMultiIndex(new StringMultiIndex(path)));
    } else if (column.getType() == DataTypes.LongType) {
      multiIndexes.put(indexName, new LongTableMultiIndex(new LongMultiIndex(path)));
    } else {
      throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
          " and " + DataTypes.LongType + " types, not " + column.getType() + ".");
    }
  }

  public OutputStream openForWrite(
      String tableName, ImmutableMap<String, BasicColumn> columns) throws ModelException, FileNotFoundException {
    tables.put(tableName, new MaterializedTableMeta(tableName, columns));
    return new FileOutputStream(config.tableDir() + File.separator + tableName);
  }

  public Resources resources(MetaResources metaResources) throws ModelException, IOException {
    ImmutableMap.Builder<String, ExposedTableReader> readers = ImmutableMap.builder();
    ImmutableMap.Builder<String, TableWriter> writers = ImmutableMap.builder();
    for (TableMetaResource resource : metaResources.tables()) {
      if (resource.isWrite()) {
        MaterializedTableMeta tableMeta = resource.getTableMeta();
        writers.put(resource.getResourceId(), new TableWriter(
            randomAccessFile(tableMeta),
            tableMeta.columns(),
            collectUniqueIndexes(tableMeta),
            collectMultiIndexes(tableMeta),
            collectForeignIndexes(tableMeta),
            collectReverseForeignIndexes(tableMeta),
            tableMeta.getSimpleChecks()));
      } else {
        MaterializedTableMeta tableMeta = resource.getTableMeta();
        readers.put(resource.getResourceId(), new MaterializedTableReader(
            new DataInputStream(fileInputStream(tableMeta)),
            tableMeta.columns()));
      }
    }
    return new Resources(readers.build(), writers.build());
  }

  public SimpleFunction<?> simpleFunction(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid function identifier " + ident.getString() + ".");
    }
    if (!simpleFunctions.containsKey(ident.getString())) {
      throw new ModelException("Function " + ident.getString() + " does not exist.");
    }
    return simpleFunctions.get(ident.getString());
  }

  public TypedAggrFunction<?> aggrFunction(Ident ident, DataType<?> argType) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid function identifier " + ident.getString() + ".");
    }
    if (!aggrFunctions.containsKey(ident.getString())) {
      throw new ModelException("Function " + ident.getString() + " does not exist.");
    }
    return aggrFunctions.get(ident.getString()).forType(argType);
  }

  public ImmutableList<String> uniqueIndexNames() {
    return ImmutableList.copyOf(uniqueIndexes.keySet());
  }

  public ImmutableList<String> multiIndexNames() {
    return ImmutableList.copyOf(multiIndexes.keySet());
  }

  public ImmutableMap<String, MaterializedTableMeta> getTables() {
    return ImmutableMap.copyOf(tables);
  }

  public static class ModelException extends Exception {
    private static final long serialVersionUID = 1L;

    public ModelException(String msg) {
      super(msg);
    }
  }

  public static class ModelRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ModelRuntimeException(Exception e) {
      super(e);
    }
  }

  public boolean tryLock(MetaResources metaResources) {
    return lockManager.tryLock(metaResources);
  }

  public void unlock(MetaResources metaResources) {
    lockManager.unlock(metaResources);
  }
}
