package com.cosyan.db.model;

import java.util.function.Function;

import javax.annotation.Nullable;

import com.cosyan.db.lang.sql.Tokens.Loc;

import lombok.Data;

@Data
public class Ident {
  private final String string;
  @Nullable
  private final Loc loc;

  public Ident(String string, Loc loc) {
    this.string = string;
    this.loc = loc;
  }

  public Ident(String string) {
    this.string = string;
    this.loc = null;
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

  public Ident map(Function<String, String> f) {
    return new Ident(f.apply(string), loc);
  }
}