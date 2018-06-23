package com.cosyan.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {

  public static final String CONF_DIR = "CONF_DIR";

  public static final String CONF_DIR_DEFAULT = "conf";

  public static final String DATA_DIR = "DATA_DIR";

  public static final String LDAP_HOST = "LDAP_HOST";

  public static final String LDAP_PORT = "LDAP_PORT";

  public static final String AUTH = "AUTH";

  private final String confDir;
  private final Properties props;

  public Config(String confDir) throws ConfigException {
    this.confDir = confDir;
    if (confDir == null) {
      throw new ConfigException("Config dir must be specified.");
    }
    String confFile = confDir + File.separator + "cosyan.db.properties";
    try {
      props = new Properties();
      props.load(new FileInputStream(new File(confFile)));
    } catch (FileNotFoundException e) {
      throw new ConfigException("Config file not found: " + confFile + ".");
    } catch (IOException e) {
      throw new ConfigException("Error loading config file: " + confFile + ".");
    }
  }

  public String metaDir() {
    return props.getProperty(DATA_DIR) + File.separator + "meta";
  }

  public String tableDir() {
    return props.getProperty(DATA_DIR) + File.separator + "table";
  }

  public String statDir() {
    return props.getProperty(DATA_DIR) + File.separator + "stat";
  }

  public String indexDir() {
    return props.getProperty(DATA_DIR) + File.separator + "index";
  }

  public String journalDir() {
    return props.getProperty(DATA_DIR) + File.separator + "journal";
  }

  public String backupDir() {
    return props.getProperty(DATA_DIR) + File.separator + "backup";
  }

  public String dataDir() {
    return props.getProperty(DATA_DIR);
  }

  public String usersFile() {
    return confDir + File.separator + "users";
  }

  public String get(String key) {
    return props.getProperty(key);
  }

  public boolean auth() {
    return bool(props.getProperty(AUTH));
  }

  public static class ConfigException extends Exception {
    private static final long serialVersionUID = 1L;

    public ConfigException(String msg) {
      super(msg);
    }
  }

  private boolean bool(String value) {
    if (value == null) {
      return false;
    } else if (value.toLowerCase().equals("true") || value.toLowerCase().equals("yes") || value.equals("1")) {
      return true;
    }
    return false;
  }
}
