package com.cosyan.db.model;

import java.util.Arrays;

import com.google.common.base.Joiner;

import lombok.Data;

@Data
public class Ident {
  private final String string;

  public String[] parts() {
    return string.split("\\.");
  }

  public boolean isSimple() {
    return parts().length == 1;
  }

  public String head() {
    return parts()[0];
  }

  public Ident tail() {
    return new Ident(Joiner.on(".").join(Arrays.copyOfRange(parts(), 1, parts().length)));
  }

  public boolean is(String str) {
    return string.equals(str);
  }

  public boolean is(char c) {
    return string.equals(String.valueOf(c));
  }

  @Override
  public String toString() {
    return string;
  }

  public String last() {
    return parts()[parts().length - 1];
  }
}