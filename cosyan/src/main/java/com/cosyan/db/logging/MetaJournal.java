package com.cosyan.db.logging;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.cosyan.db.conf.Config;
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

  public void reload(Session session) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(fileName()));
    String line = null;
    while ((line = reader.readLine()) != null) {
      session.execute(line);
    }
    reader.close();
  }
}
