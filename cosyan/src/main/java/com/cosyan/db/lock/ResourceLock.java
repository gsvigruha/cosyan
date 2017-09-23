package com.cosyan.db.lock;

import com.cosyan.db.sql.SyntaxTree.Ident;

import lombok.Data;

@Data
public class ResourceLock {

  private final String resourceId;
  private final boolean read;
  private final boolean write;

  public static ResourceLock readWrite(Ident ident) {
    return new ResourceLock(ident.getString(), true, true);
  }
  
  public static ResourceLock read(Ident ident) {
    return new ResourceLock(ident.getString(), true, false);
  }
}
