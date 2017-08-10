package com.cosyan.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public class Config {

  public static final String CONF_FILE = "CONF_FILE";

  public static final String CONF_FILE_DEFAULT = "conf" + File.separator + "cosyan.db.properties";

  public static final String DATA_DIR = "DATA_DIR";

  private final Properties props;

  public Config() throws ConfigException {
    String file = Optional.ofNullable(System.getenv("CONF_FILE")).orElse(CONF_FILE_DEFAULT);
    try {
      props = new Properties();
      props.load(new FileInputStream(new File(file)));
    } catch (FileNotFoundException e) {
      throw new ConfigException("Config file not found: " + file + ".");
    } catch (IOException e) {
      throw new ConfigException("Error loading config file: " + file + ".");
    }
  }

  public Config(Properties props) {
    this.props = props;
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
