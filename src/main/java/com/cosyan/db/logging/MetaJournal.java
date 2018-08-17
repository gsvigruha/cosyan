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
