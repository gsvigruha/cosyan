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
package com.cosyan.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;

import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableList;

public class Config {

  @ConfigType(type = ConfigType.FILE, mandatory = true, doc = "The directory containing the config files.", hidden = true)
  public static final String DATA_DIR = "DATA_DIR";

  @ConfigType(type = ConfigType.STRING, mandatory = false, doc = "The hostame of the LDAP server.", hidden = true)
  public static final String LDAP_HOST = "LDAP_HOST";

  @ConfigType(type = ConfigType.INT, mandatory = false, doc = "The port of the LDAP server.", hidden = true)
  public static final String LDAP_PORT = "LDAP_PORT";

  @ConfigType(type = ConfigType.BOOL, mandatory = true, doc = "Whether authentication is enabled or not.")
  public static final String AUTH = "AUTH";

  @ConfigType(type = ConfigType.INT, mandatory = true, doc = "The port Cosyan server listens on.")
  public static final String PORT = "PORT";

  @ConfigType(type = ConfigType.INT, mandatory = true, doc = "The number of threads for the webserver.")
  public static final String WEBSERVER_NUM_THREADS = "WEBSERVER_NUM_THREADS";

  @ConfigType(type = ConfigType.INT, mandatory = true, doc = "The number of threads for the DB.")
  public static final String DB_NUM_THREADS = "DB_NUM_THREADS";

  @ConfigType(type = ConfigType.INT, mandatory = true, doc = "The amount of time tasks sleep before trying to acquire locks again.")
  public static final String TR_RETRY_MS = "TR_RETRY_MS";

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

  public boolean auth() throws ConfigException {
    return bool(props.getProperty(AUTH));
  }

  public int port() throws ConfigException {
    return integer(props.getProperty(PORT));
  }

  public static class ConfigException extends Exception {
    private static final long serialVersionUID = 1L;

    public ConfigException(String msg) {
      super(msg);
    }
  }

  private boolean bool(String value) throws ConfigException {
    try {
      return DataTypes.boolFromString(value);
    } catch (RuleException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  private int integer(String value) throws ConfigException {
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  public int getInt(String key) throws ConfigException {
    if (!props.containsKey(key)) {
      throw new ConfigException(String.format("Missing config %s.", key));
    }
    return integer(props.getProperty(key));
  }

  public static ImmutableList<Field> fields(boolean showHidden) {
    ImmutableList.Builder<Field> builder = ImmutableList.builder();
    for (Field field : Config.class.getFields()) {
      ConfigType configType = field.getAnnotation(ConfigType.class);
      if (configType != null && (!configType.hidden() || showHidden)) {
        builder.add(field);
      }
    }
    return builder.build();
  }
}
