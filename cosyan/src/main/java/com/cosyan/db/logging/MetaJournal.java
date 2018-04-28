package com.cosyan.db.logging;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.cosyan.db.conf.Config;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.session.Session;

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

  public void reload(Session session) throws IOException, DBException {
    BufferedReader reader = new BufferedReader(new FileReader(fileName()));
    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        Result result = session.execute(line);
        if (result instanceof ErrorResult) {
          throw new DBException(((ErrorResult) result).getError());
        }
        if (result instanceof CrashResult) {
          throw new DBException(((CrashResult) result).getError());
        }
      }
    } finally {
      reader.close();
    }
  }

  public static class DBException extends Exception {

    private static final long serialVersionUID = 1L;

    public DBException(Throwable cause) {
      super("DB crashed during start: " + cause.getMessage(), cause);
    }
  }

}
