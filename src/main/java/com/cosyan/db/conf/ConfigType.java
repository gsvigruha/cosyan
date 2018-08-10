package com.cosyan.db.conf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface ConfigType {

  public static String FILE = "FILE";

  public static String STRING = "STRING";

  public static String INT = "INT";

  public static String BOOL = "BOOL";

  String type();

  boolean mandatory();

  String doc();
}
