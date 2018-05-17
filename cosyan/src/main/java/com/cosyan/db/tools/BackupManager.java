package com.cosyan.db.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;

import com.cosyan.db.conf.Config;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.meta.MetaRepo;

public class BackupManager {

  private final Config config;
  private final MetaRepo metaRepo;

  public BackupManager(Config config, MetaRepo metaRepo) {
    this.config = config;
    this.metaRepo = metaRepo;
  }

  private void backupDir(String srcDir, String dstDir, ZipOutputStream stream) throws IOException {
    File dir = new File(srcDir);
    for (String path : dir.list()) {
      backupFile(dir.getAbsolutePath() + File.separator + path, dstDir + File.separator + path, stream);
    }
  }

  private void backupFile(String srcPath, String entryPath, ZipOutputStream stream) throws IOException {
    ZipEntry zipEntry = new ZipEntry(entryPath);
    stream.putNextEntry(zipEntry);
    Files.copy(Paths.get(srcPath), stream);
  }

  public void backup(String name) throws IOException {
    metaRepo.metaRepoReadLock();
    try {
      metaRepo.writeTables();
      Files.createDirectories(Paths.get(config.backupDir()));
      ZipOutputStream stream = new ZipOutputStream(
          Files.newOutputStream(Paths.get(config.backupDir() + File.separator + name)));
      try {
        backupDir(config.metaDir(), "meta", stream);
        backupDir(config.tableDir(), "table", stream);
        backupDir(config.indexDir(), "index", stream);
        backupFile(config.usersFile(), "users", stream);
        stream.closeEntry();
      } finally {
        stream.close();
      }
    } finally {
      metaRepo.metaRepoReadUnlock();
    }
  }

  public void restore(String name) throws IOException, DBException {
    metaRepo.metaRepoWriteLock();
    try {
      FileUtils.deleteDirectory(new File(config.tableDir()));
      FileUtils.deleteDirectory(new File(config.indexDir()));
      FileUtils.deleteDirectory(new File(config.metaDir()));
      FileUtils.forceDelete(new File(config.usersFile()));

      Files.createDirectories(Paths.get(config.tableDir()));
      Files.createDirectories(Paths.get(config.indexDir()));
      Files.createDirectories(Paths.get(config.metaDir()));

      ZipInputStream stream = new ZipInputStream(
          Files.newInputStream(Paths.get(config.backupDir() + File.separator + name)));
      try {
        ZipEntry entry;
        while ((entry = stream.getNextEntry()) != null) {
          FileUtils.copyToFile(stream, new File(config.confDir() + File.separator + entry.getName()));
        }
      } finally {
        stream.close();
      }
      metaRepo.readTables();
    } finally {
      metaRepo.metaRepoWriteUnlock();
    }
  }
}
