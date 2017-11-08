package com.cosyan.db.model;

import lombok.Data;

@Data
public class Ident {
  private final String string;

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
}