package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.DataType;
import com.sharingapples.sync.resource.FieldMap;
import com.sharingapples.sync.resource.ResourceMap;
import com.sharingapples.sync.resource.ResourceMarker;
import com.sharingapples.sync.state.State;
import com.sharingapples.sync.store.StoreException;

import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by ranjan on 12/13/15.
 */
public class StoreSqlite extends StoreJDBC {
  public StoreSqlite(File file) throws StoreException {
    super("org.sqlite.JDBC", "jdbc:sqlite:" + file.getAbsolutePath(), new Properties());
  }


}
