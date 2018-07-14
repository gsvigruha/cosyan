package com.cosyan.db.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.cosyan.db.conf.Config;

public class MetaJournal {

  private final Config config;
  private BufferedWriter stream;

  public MetaJournal(Config config) throws IOException {
    this.config = config;
    Files.createDirectories(Paths.get(config.journalDir()));
    this.stream = new BufferedWriter(new FileWriter(fileName(), /* append= */true));
  }

  private String fileName() {
    return config.journalDir() + File.separator + "meta.journal";
  }

  public synchronized void log(String sql) throws IOException {
    stream.write(sql.replace("\n", " "));
    stream.newLine();
    stream.flush();
  }

  public static class DBException extends Exception {

    private static final long serialVersionUID = 1L;

    public DBException(Throwable cause) {
      super("DB crashed during start: " + cause.getMessage(), cause);
    }
  }

}
