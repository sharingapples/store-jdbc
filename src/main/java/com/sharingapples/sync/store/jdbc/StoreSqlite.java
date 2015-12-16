package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.Registrar;
import com.sharingapples.sync.store.StoreException;

import java.io.File;
import java.util.Properties;

/**
 * Created by ranjan on 12/13/15.
 */
public class StoreSqlite extends StoreJDBC {
  public StoreSqlite(Registrar registrar, File dbFile) throws StoreException {
    super(registrar, "org.sqlite.JDBC",
            "jdbc:sqlite:" + dbFile.getAbsolutePath(), new Properties());
  }


}
