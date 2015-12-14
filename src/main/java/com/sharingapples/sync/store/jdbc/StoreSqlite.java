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

  public <T extends ResourceMarker> void createTable(Class<T> clazz) {
    ResourceMap<T> map = State.getResourceMap(clazz);

    String sql = "CREATE TABLE IF NOT EXISTS " + quoteSystemIdentifier(map.getName()) + "(";
    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap = map.getFieldMap(i);
      if (fieldMap.getType().isMany()) {
        continue;
      }

      if (i > 0) {
        sql += ",";
      }

      sql += fieldMap.getName() + " " + getTypeName(fieldMap.getType());
      if (fieldMap == map.getPrimaryField()) {
        sql += " PRIMARY KEY AUTOINCREMENT";
      }
    }
    sql += ");";

    try {
      Statement stmt = getConnection().createStatement();
      stmt.executeUpdate(sql);
    } catch(SQLException e) {
      throw new StoreException("Error while executing " + sql, e);
    }
  }

  public String getTypeName(DataType type) {
    if (type == DataType.INT || type == DataType.LONG) {
      return "INTEGER";
    } else if (type == DataType.STRING) {
      return "TEXT";
    } else if (type == DataType.DATE) {
      return "DATE";
    } else if (type.isReference()) {
      return "INT";
    } else if (type.isMany()) {
      throw new StoreException("A Many type is relation and not a field");
    } else {
      throw new StoreException("Unsupported Data Type " + type);
    }
  }
}
