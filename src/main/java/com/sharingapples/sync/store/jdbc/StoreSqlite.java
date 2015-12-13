package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.store.StoreException;

import java.io.File;

/**
 * Created by ranjan on 12/13/15.
 */
public class StoreSqlite extends StoreJDBC {
  public StoreSqlite(File file) throws StoreException {
    super("org.sqlite.JDBC", "jdbc:sqlite:" + file.getAbsolutePath(), null);
  }
}
