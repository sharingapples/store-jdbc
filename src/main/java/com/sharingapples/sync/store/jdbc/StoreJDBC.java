package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.FieldMap;
import com.sharingapples.sync.resource.ResourceMap;
import com.sharingapples.sync.resource.ResourceMarker;
import com.sharingapples.sync.store.Store;
import com.sharingapples.sync.store.StoreException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Properties;

/**
 * Created by ranjan on 12/12/15.
 */
public class StoreJDBC extends Store {

  private Connection db;
  private final String connUrl;
  private final Properties connProps;

  public StoreJDBC(String driverClass, String connUrl, Properties connProps) throws StoreException {
    super();

    try {
      Class.forName(driverClass);
    } catch(ClassNotFoundException e) {
      throw new StoreException("The JDBC driver class - " + driverClass + " was not found", e);
    }

    this.connUrl = connUrl;
    this.connProps = connProps;

    try {
      db = DriverManager.getConnection(connUrl, connProps);
    } catch(SQLException e) {
      throw new StoreException("Could not connect to the target database using " + connUrl, e);
    }

  }

  public void reconnect() {
    close();
    try {
      db = DriverManager.getConnection(connUrl, connProps);
    } catch(SQLException e) {
      throw new StoreException("Error while reconnecting to the target database " + connUrl, e);
    }
  }

  public void close() {
    try {
      db.close();
    } catch(SQLException e) {
      LOGGER.error("An error occurred while trying to close database connection", e);
    }
  }

  protected String quoteSystemIdentifier(String identifier) {
    return "\"" + identifier + "\"";
  }

  @Override
  public <T extends ResourceMarker> JDBCRecordSet<T> fetchAll(ResourceMap<T> map) throws StoreException {
    String sql = "SELECT * FROM " + quoteSystemIdentifier(map.getName());

    PreparedStatement stmt;
    ResultSet rs;
    try {
      stmt = db.prepareStatement(sql);
      rs = stmt.executeQuery();
      stmt.close();
    } catch(SQLException e) {
      throw new StoreException("Error while executing sql - " + sql, e);
    }

    return new JDBCRecordSet(map, rs);
  }

  @Override
  public <T extends ResourceMarker> T fetch(ResourceMap<T> map, T resource) {
    String sql = "SELECT * FROM " + quoteSystemIdentifier(map.getName())
            + " WHERE " + quoteSystemIdentifier(map.getPrimaryField().getName())
            + "=?";

    ResultSet rs;
    try {
      PreparedStatement stmt = db.prepareStatement(sql);
      map.getPrimaryField().getType().toJDBC(stmt, 1, resource.getId());
      rs = stmt.executeQuery();
    } catch(SQLException e) {
      throw new StoreException("Error while executing - " + sql, e);
    }

    // Create a recordset that does all the transformation
    JDBCRecordSet<T> recordSet = new JDBCRecordSet<T>(map, rs);

    return recordSet.next();
  }

  @Override
  public <T extends ResourceMarker> T insert(ResourceMap<T> map, T resource) throws StoreException {

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("INSERT INTO ");
    sqlBuilder.append(quoteSystemIdentifier(map.getName()));
    sqlBuilder.append('(');
    for(int i=0; i<map.getFieldsCount(); ++i) {
      if (i>0) {
        sqlBuilder.append(',');
      }
      FieldMap fieldMap =  map.getFieldMap(i);
      sqlBuilder.append(quoteSystemIdentifier(fieldMap.getName()));
    }
    sqlBuilder.append(") VALUES (");
    for(int i=0; i<map.getFieldsCount(); ++i) {
      if (i>0) {
        sqlBuilder.append(',');
      }
      sqlBuilder.append('?');
    }
    sqlBuilder.append(')');

    PreparedStatement stmt;
    try {
      stmt = db.prepareStatement(sqlBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
    } catch(SQLException e) {
      throw new StoreException("Error while preparing " + sqlBuilder.toString(), e);
    }

    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap = map.getFieldMap(i);
      fieldMap.getType().toJDBC(stmt, i+1, fieldMap.get(resource));
    }

    int affectedRows;
    try {
      affectedRows = stmt.executeUpdate();
    } catch(SQLException e) {
      throw new StoreException("Error while executing " + sqlBuilder.toString(), e);
    }
    if (affectedRows != 1) {
      throw new StoreException("Could not retrieve the automatically generated id");
    }

    try {
      ResultSet keys = stmt.getGeneratedKeys();
      if (keys.next()) {
        map.getPrimaryField().set(resource, map.getPrimaryField().getType().fromJDBC(keys, 1));
        return map.find(resource.getId(), resource);
      } else {
        throw new StoreException("Generated Key not found while inserting new record for " + map.getName());
      }
    } catch(SQLException e) {
      throw new StoreException("Error while trying to retrieve generated keys", e);
    }

  }

  @Override
  public <T extends ResourceMarker> T update(ResourceMap<T> map, T resource) {
    throw new UnsupportedOperationException("Not implemented yet. Please update now.");
  }

  @Override
  public <T extends ResourceMarker> T delete(ResourceMap<T> map, T resource)
          throws StoreException {
    String sql = "DELETE FROM " + quoteSystemIdentifier(map.getName()) +
            "WHERE " + quoteSystemIdentifier(map.getPrimaryField().getName()) + "=?";

    PreparedStatement stmt = null;
    try {
      stmt = db.prepareStatement(sql);
    } catch (SQLException e) {
      throw new StoreException("Error while preparing sql - " + sql, e);
    }
    map.getPrimaryField().getType().toJDBC(stmt, 1, resource.getId());

    int affectedRows = 0;
    try {
      affectedRows = stmt.executeUpdate();
    } catch (SQLException e) {
      throw new StoreException("Error while executing sql - " + sql, e);
    }

    if (affectedRows != 1) {
      throw new StoreException("Delete failed. " + affectedRows + " records deleted from " + map.getName() + " while trying to delete " + resource.toString());
    }

    // clear from the cache as well
    map.delete(resource);

    return resource;
  }
}
