package com.cosyan.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Optional;

public class Config {

  public static final String CONF_FILE = "CONF_FILE";

  public static final String CONF_FILE_DEFAULT = "conf" + File.separator + "cosyan.db.properties";

  public static final String DATA_DIR = "DATA_DIR";

  private final Properties props = new Properties();

  public Config() throws ConfigException {
    String file = Optional.of(System.getenv("CONF_FILE")).or(CONF_FILE_DEFAULT);
    try {
      props.load(new FileInputStream(new File(file)));
    } catch (FileNotFoundException e) {
      throw new ConfigException("Config file not found: " + file + ".");
    } catch (IOException e) {
      throw new ConfigException("Error loading config file: " + file + ".");
    }
  }

  public String dataDir() {
    return props.getProperty(DATA_DIR);
  }

  public static class ConfigException extends Exception {
    public ConfigException(String msg) {
      super(msg);
    }
  }
}
