package com.cosyan.db.meta;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import com.cosyan.db.conf.Config;
import com.cosyan.db.index.ByteMultiTrie.LongMultiIndex;
import com.cosyan.db.index.ByteMultiTrie.StringMultiIndex;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;
import com.cosyan.db.index.IDIndex;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableIndex.IDTableIndex;
import com.cosyan.db.model.TableIndex.LongTableIndex;
import com.cosyan.db.model.TableIndex.StringTableIndex;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.model.TableMultiIndex.LongTableMultiIndex;
import com.cosyan.db.model.TableMultiIndex.StringTableMultiIndex;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;
import com.cosyan.db.transaction.Resources;
import com.cosyan.db.util.Util;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class MetaRepo implements TableProvider {

  private final Config config;
  private final HashMap<String, MaterializedTableMeta> tables;
  private final HashMap<String, TableIndex> uniqueIndexes;
  private final HashMap<String, TableMultiIndex> multiIndexes;
  private final Grants grants;

  private final LockManager lockManager;

  public MetaRepo(Config config, LockManager lockManager) throws IOException, ModelException, ParserException {
    this.config = config;
    this.lockManager = lockManager;
    this.tables = new HashMap<>();
    this.uniqueIndexes = new HashMap<>();
    this.multiIndexes = new HashMap<>();
    this.grants = new Grants();

    Files.createDirectories(Paths.get(config.dataDir()));
    Files.createDirectories(Paths.get(config.tableDir()));
    Files.createDirectories(Paths.get(config.indexDir()));
    Files.createDirectories(Paths.get(config.journalDir()));
  }

  public Config config() {
    return config;
  }

  @Override
  public ExposedTableMeta tableMeta(Ident ident) throws ModelException {
    return table(ident).reader();
  }

  public MaterializedTableMeta table(Ident ident) throws ModelException {
    if (!tables.containsKey(ident.getString())) {
      throw new ModelException("Table '" + ident.getString() + "' does not exist.");
    }
    return tables.get(ident.getString());
  }

  public ImmutableMap<String, IndexReader> collectIndexReaders(MaterializedTableMeta table) {
    ImmutableMap.Builder<String, IndexReader> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      if (column.isIndexed()) {
        String indexName = table.tableName() + "." + column.getName();
        if (column.isUnique()) {
          builder.put(column.getName(), uniqueIndexes.get(indexName));
        } else {
          builder.put(column.getName(), multiIndexes.get(indexName));
        }
      }
    }
    return builder.build();
  }

  @VisibleForTesting
  public ImmutableMap<String, TableIndex> collectUniqueIndexes(MaterializedTableMeta table) {
    ImmutableMap.Builder<String, TableIndex> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      String indexName = table.tableName() + "." + column.getName();
      if (column.isIndexed() && column.isUnique()) {
        builder.put(column.getName(), uniqueIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  @VisibleForTesting
  public ImmutableMap<String, TableMultiIndex> collectMultiIndexes(MaterializedTableMeta table) {
    ImmutableMap.Builder<String, TableMultiIndex> builder = ImmutableMap.builder();
    for (BasicColumn column : table.columns().values()) {
      String indexName = table.tableName() + "." + column.getName();
      if (column.isIndexed() && !column.isUnique()) {
        builder.put(column.getName(), multiIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  public ImmutableMultimap<String, IndexReader> collectForeignIndexes(MaterializedTableMeta table) {
    ImmutableMultimap.Builder<String, IndexReader> builder = ImmutableMultimap.builder();
    for (ForeignKey foreignKey : table.foreignKeys().values()) {
      builder.put(
          foreignKey.getColumn().getName(),
          uniqueIndexes.get(foreignKey.getRefTable().tableName() + "." + foreignKey.getRefColumn().getName()));
    }
    return builder.build();
  }

  public ImmutableMultimap<String, IndexReader> collectReverseForeignIndexes(MaterializedTableMeta table) {
    ImmutableMultimap.Builder<String, IndexReader> builder = ImmutableMultimap.builder();
    for (ReverseForeignKey reverseForeignKey : table.reverseForeignKeys().values()) {
      String indexName = reverseForeignKey.getRefTable().tableName() + "."
          + reverseForeignKey.getRefColumn().getName();
      if (reverseForeignKey.getRefColumn().isUnique()) {
        builder.put(reverseForeignKey.getColumn().getName(), uniqueIndexes.get(indexName));
      } else {
        builder.put(reverseForeignKey.getColumn().getName(), multiIndexes.get(indexName));
      }
    }
    return builder.build();
  }

  public void registerTable(MaterializedTableMeta tableMeta) throws IOException {
    String tableName = tableMeta.tableName();
    tables.put(tableName, tableMeta);
    lockManager.registerLock(tableName);
  }

  public void dropTable(String tableName) throws IOException {
    tables.remove(tableName);
    lockManager.removeLock(tableName);
  }

  public boolean hasTable(String tableName) {
    return tables.containsKey(tableName);
  }

  public void registerUniqueIndex(MaterializedTableMeta table, BasicColumn column) throws ModelException, IOException {
    String indexName = table.tableName() + "." + column.getName();
    String path = config.indexDir() + File.separator + indexName;
    assert !uniqueIndexes.containsKey(indexName);
    if (column.isUnique()) {
      if (column.getType() == DataTypes.StringType) {
        uniqueIndexes.put(indexName, new StringTableIndex(new StringIndex(path)));
        lockManager.registerLock(indexName);
      } else if (column.getType() == DataTypes.LongType) {
        uniqueIndexes.put(indexName, new LongTableIndex(new LongIndex(path)));
        lockManager.registerLock(indexName);
      } else if (column.getType() == DataTypes.IDType) {
        uniqueIndexes.put(indexName, new IDTableIndex(new IDIndex(path)));
        lockManager.registerLock(indexName);
      } else {
        throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
            " and " + DataTypes.LongType + " types, not " + column.getType() + ".");
      }
    } else {
      throw new ModelException("Column " + column.getName() + " is not unique.");
    }
  }

  public void registerMultiIndex(MaterializedTableMeta table, BasicColumn column) throws ModelException, IOException {
    String indexName = table.tableName() + "." + column.getName();
    String path = config.indexDir() + File.separator + indexName;
    assert !multiIndexes.containsKey(indexName);
    if (column.getType() == DataTypes.StringType) {
      multiIndexes.put(indexName, new StringTableMultiIndex(new StringMultiIndex(path)));
      lockManager.registerLock(indexName);
    } else if (column.getType() == DataTypes.LongType || column.getType() == DataTypes.IDType) {
      multiIndexes.put(indexName, new LongTableMultiIndex(new LongMultiIndex(path)));
      lockManager.registerLock(indexName);
    } else {
      throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
          " and " + DataTypes.LongType + " types, not " + column.getType() + ".");
    }
  }

  public void dropUniqueIndex(MaterializedTableMeta table, BasicColumn column) throws IOException {
    String indexName = table.tableName() + "." + column.getName();
    TableIndex index = uniqueIndexes.remove(indexName);
    lockManager.removeLock(indexName);
    index.drop();
  }

  public void dropMultiIndex(MaterializedTableMeta table, BasicColumn column) throws IOException {
    String indexName = table.tableName() + "." + column.getName();
    TableMultiIndex index = multiIndexes.remove(indexName);
    lockManager.removeLock(indexName);
    index.drop();
  }

  public OutputStream openForWrite(
      String tableName, ImmutableMap<String, BasicColumn> columns) throws ModelException, FileNotFoundException {
    return new FileOutputStream(config.tableDir() + File.separator + tableName);
  }

  public Resources resources(MetaResources metaResources) throws IOException {
    ImmutableMap.Builder<String, SeekableTableReader> readers = ImmutableMap.builder();
    ImmutableMap.Builder<String, TableWriter> writers = ImmutableMap.builder();
    for (TableMetaResource resource : metaResources.tables()) {
      if (resource.isWrite()) {
        MaterializedTableMeta tableMeta = resource.getTableMeta();
        writers.put(resource.getTableMeta().tableName(), new TableWriter(
            tableMeta,
            tableMeta.fileName(),
            tableMeta.fileWriter(),
            tableMeta.fileReader(),
            tableMeta.allColumns(),
            collectUniqueIndexes(tableMeta),
            collectMultiIndexes(tableMeta),
            resource.isForeignIndexes() ? collectForeignIndexes(tableMeta) : ImmutableMultimap.of(),
            resource.isReverseForeignIndexes() ? collectReverseForeignIndexes(tableMeta) : ImmutableMultimap.of(),
            ImmutableMap.copyOf(tableMeta.rules()),
            tableMeta.reverseRuleDependencies(),
            tableMeta.primaryKey()));
      } else {
        MaterializedTableMeta tableMeta = resource.getTableMeta();
        readers.put(resource.getTableMeta().tableName(), new MaterializedTableReader(
            tableMeta,
            tableMeta.fileName(),
            tableMeta.fileReader(),
            tableMeta.allColumns(),
            collectIndexReaders(tableMeta)));
      }
    }
    return new Resources(readers.build(), writers.build());
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

  public static class RuleException extends Exception {
    private static final long serialVersionUID = 1L;

    public RuleException(String msg) {
      super(msg);
    }

    public RuleException(IndexException e) {
      super(e.getMessage());
    }
  }

  public void metaRepoReadLock() {
    lockManager.metaRepoReadLock();
  }

  public void metaRepoWriteLock() {
    lockManager.metaRepoWriteLock();
  }

  public void metaRepoReadUnlock() {
    lockManager.metaRepoReadUnlock();
  }

  public void metaRepoWriteUnlock() {
    lockManager.metaRepoWriteUnlock();
  }

  public boolean tryLock(MetaResources metaResources) {
    return lockManager.tryLock(metaResources);
  }

  public void unlock(MetaResources metaResources) {
    lockManager.unlock(metaResources);
  }

  public ImmutableMap<String, ByteTrieStat> uniqueIndexStats() throws IOException {
    return Util.<String, TableIndex, ByteTrieStat>mapValuesIOException(uniqueIndexes, TableIndex::stats);
  }

  public ImmutableMap<String, ByteMultiTrieStat> multiIndexStats() throws IOException {
    return Util.<String, TableMultiIndex, ByteMultiTrieStat>mapValuesIOException(multiIndexes, TableMultiIndex::stats);
  }
}
