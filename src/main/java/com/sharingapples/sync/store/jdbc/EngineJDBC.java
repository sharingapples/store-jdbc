package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.*;
import com.sharingapples.sync.state.State;
import com.sharingapples.sync.store.Engine;
import com.sharingapples.sync.store.Store;
import com.sharingapples.sync.store.StoreException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ranjan on 12/14/15.
 */
public class EngineJDBC extends Engine {

  private final Connection connection;
  EngineJDBC(Connection connection) {
    this.connection = connection;
    try {
      this.connection.setAutoCommit(false);
    } catch(SQLException e) {
      throw new StoreException("Could not start transaction", e);
    }
  }

  protected String quoteSystemIdentifier(String identifier) {
    return "\"" + identifier + "\"";
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

    try (Statement stmt = connection.createStatement()) {
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

  @Override
  public void commit() {
    try {
      connection.commit();
    } catch(SQLException e) {
      throw new StoreException("Error while committing session", e);
    }
  }

  @Override
  public void close() {
    try {
      connection.rollback();
      connection.close();
    } catch(SQLException e) {
      throw new StoreException("Error while closing engine connection", e);
    }
  }

  @Override
  public <T extends ResourceMarker> JDBCRecordSet<T> fetchAll(ResourceMap<T> map) throws StoreException {
    String sql = "SELECT * FROM " + quoteSystemIdentifier(map.getName());

    PreparedStatement stmt;
    ResultSet rs;
    try {
      stmt = connection.prepareStatement(sql);
      rs = stmt.executeQuery();
    } catch(SQLException e) {
      throw new StoreException("Error while executing sql - " + sql, e);
    }

    return new JDBCRecordSet<T>(stmt, map, rs);
  }

  @Override
  public <T extends ResourceMarker> T fetch(ResourceMap<T> map, Object id) {
    String sql = "SELECT * FROM " + quoteSystemIdentifier(map.getName())
            + " WHERE " + quoteSystemIdentifier(map.getPrimaryField().getName())
            + "=?";

    PreparedStatement stmt;
    ResultSet rs;
    try {
      stmt = connection.prepareStatement(sql);
      map.getPrimaryField().getType().toJDBC(stmt, 1, id);
      rs = stmt.executeQuery();
    } catch(SQLException e) {
      throw new StoreException("Error while executing - " + sql, e);
    }

    // Create a RecordSet that does all the transformation
    JDBCRecordSet<T> recordSet = new JDBCRecordSet<T>(stmt, map, rs);

    if (recordSet.hasNext()) {
      T res = recordSet.next();

      // If there are more than one records, then raise an error flag and iterate
      // through them, to make sure the record set is closed properly
      while(recordSet.hasNext()) {
        Store.LOGGER.warn("More than one record retrieved in fetch for " + map.getName() + " for id " + id);
      }

      return res;
    } else {
      return null;
    }
  }

  @Override
  public <T extends ResourceMarker> T insert(ResourceMap<T> map, T resource) throws StoreException {

    StringBuilder sqlBuilder = new StringBuilder();
    StringBuilder placeHolders = new StringBuilder();
    sqlBuilder.append("INSERT INTO ");
    sqlBuilder.append(quoteSystemIdentifier(map.getName()));
    sqlBuilder.append('(');
    boolean first = true;
    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap =  map.getFieldMap(i);

      // skip the many relationships
      if (fieldMap.getType().isMany()) {
        continue;
      }

      if (!first) {
        sqlBuilder.append(',');
        placeHolders.append(',');
      } else {
        first = false;
      }
      sqlBuilder.append(quoteSystemIdentifier(fieldMap.getName()));
      placeHolders.append('?');
    }

    sqlBuilder.append(") VALUES (");
    sqlBuilder.append(placeHolders);
    sqlBuilder.append(");");

    List<Many> manyList = new ArrayList<>();
    // insert all the values
    try (PreparedStatement stmt = connection.prepareStatement(sqlBuilder.toString(), Statement.RETURN_GENERATED_KEYS)) {
      int columnIndex = 0;
      for (int i = 0; i < map.getFieldsCount(); ++i) {
        FieldMap fieldMap = map.getFieldMap(i);
        if (fieldMap.getType().isMany()) {
          manyList.add((Many)fieldMap.get(resource));
          continue;
        }

        Object fieldValue = fieldMap.get(resource);

        if (fieldMap.getType().isReference()) {
          // in case of reference, we might need to do a recursive insert for
          // references without
          if (fieldValue != null && ((ResourceMarker)fieldValue).getId() == null) {
            fieldValue = insert((ResourceMap)fieldMap.getType(), (ResourceMarker)fieldValue);
          }
        }

        if (fieldValue == null) {
          stmt.setObject(++columnIndex, null);
        } else {
          fieldMap.getType().toJDBC(stmt, ++columnIndex, fieldValue);
        }
      }

      int affectedRows;
      try {
        affectedRows = stmt.executeUpdate();
      } catch (SQLException e) {
        throw new StoreException("Error while executing " + sqlBuilder.toString(), e);
      }
      if (affectedRows != 1) {
        throw new StoreException("Could not retrieve the automatically generated id");
      }

      try (ResultSet keys = stmt.getGeneratedKeys()) {
        if (keys.next()) {
          map.getPrimaryField().set(resource, map.getPrimaryField().getType().fromJDBC(keys, 1));
          T res =  map.find(resource.getId(), resource);

          checkAndUpdate(manyList);
          return res;
        } else {
          throw new StoreException("Generated Key not found while inserting new record for " + map.getName());
        }
      } catch (SQLException e) {
        throw new StoreException("Error while trying to retrieve generated keys", e);
      }

    } catch (SQLException e) {
      throw new StoreException("Error while preparing " + sqlBuilder.toString(), e);
    }
  }

  @Override
  public <T extends ResourceMarker> T update(ResourceMap<T> map, T resource) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("UPDATE ");
    sqlBuilder.append(quoteSystemIdentifier(map.getName()));
    sqlBuilder.append(" SET");

    boolean first = true;
    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap = map.getFieldMap(i);

      if (fieldMap.getType().isMany() || fieldMap == map.getPrimaryField()) {
        // Skip the Many relationship and the primary key field
        continue;
      }

      if (!first) {
        sqlBuilder.append(',');
      } else {
        first = false;
      }
      sqlBuilder.append(fieldMap.getName());
      sqlBuilder.append("=?");
    }

    sqlBuilder.append(" WHERE ");
    sqlBuilder.append(map.getPrimaryField().getName());
    sqlBuilder.append("=?");

    try (PreparedStatement stmt = connection.prepareStatement(sqlBuilder.toString())) {
      // fill up all the '?';
      int columnIndex = 0;
      for(int i=0; i<map.getFieldsCount(); ++i) {
        FieldMap fieldMap = map.getFieldMap(i);

        if (fieldMap.getType().isMany() || fieldMap == map.getPrimaryField()) {
          // Skip the Many relationships and the primary key field
          continue;
        }

        Object fieldValue = fieldMap.get(resource);
        if (fieldMap.getType().isReference()) {
          // Check if the referenced type is inserted in the database or not
          // if not do the insertion now
          if (fieldValue != null && ((ResourceMarker)fieldValue).getId() == null) {
            fieldValue = insert((ResourceMap)fieldMap.getType(), (ResourceMarker)fieldValue);
          }
        }

        fieldMap.getType().toJDBC(stmt, ++columnIndex, fieldValue);
      }

      // Set the primary key value
      map.getPrimaryField().getType().toJDBC(stmt, ++columnIndex, resource.getId());

      try {
        stmt.executeUpdate();
      } catch(SQLException e) {
        throw new StoreException("Error while executing sql - " + sqlBuilder.toString(), e);
      }

      return resource;

    } catch(SQLException e) {
      throw new StoreException("Error while preparing sql - " + sqlBuilder.toString(), e);
    }
  }

  private void checkAndUpdate(List<Many> manyList) {
    for(Many item:manyList) {

    }
  }

  @Override
  public <T extends ResourceMarker> T delete(ResourceMap<T> map, T resource)
          throws StoreException {
    String sql = "DELETE FROM " + quoteSystemIdentifier(map.getName()) +
            "WHERE " + quoteSystemIdentifier(map.getPrimaryField().getName()) + "=?";

    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sql);
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
    map.remove(resource);

    return resource;
  }
}
