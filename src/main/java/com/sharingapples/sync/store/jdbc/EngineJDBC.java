package com.sharingapples.sync.store.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sharingapples.sync.resource.*;
import com.sharingapples.sync.state.State;
import com.sharingapples.sync.store.Engine;
import com.sharingapples.sync.store.Store;
import com.sharingapples.sync.store.StoreException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The JDBC Engine for storing/retrieving data from JDBC specific databases
 *
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
      if (fieldMap.getType().isMany() || fieldMap.isTransient()) {
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

  private Object insertOrUpdateReference(ResourceMap map, ObjectNode node) {
    JsonNode idNode = node.get(map.getPrimaryField().getName());
    if (idNode == null || idNode.isNull()) {
      // Definitely an insert
      return insert(map, node);
    } else {
      // It could be an update or an insert (Special case where id may be reference to another resource)
      if (idNode.isObject() && map.getPrimaryField().getType().isReference()) {
        return insertOrUpdateReference((ResourceMap)map.getPrimaryField().getType(), (ObjectNode)idNode);
      } else {
        return update(map, node);
        //fieldValue = referenced.getPrimaryField().getType().fromJson(idNode);
      }
    }
  }

  @Override
  public Object insert(ResourceMap map, ObjectNode node) throws StoreException {

    StringBuilder sqlBuilder = new StringBuilder();
    StringBuilder placeHolders = new StringBuilder();
    sqlBuilder.append("INSERT INTO ");
    sqlBuilder.append(quoteSystemIdentifier(map.getName()));
    sqlBuilder.append('(');
    boolean first = true;
    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap =  map.getFieldMap(i);

      // skip the many relationships
      if (fieldMap.getType().isMany() || fieldMap.isTransient()) {
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
          //manyList.add((Many)fieldMap.get(resource));
          continue;
        }

        if (fieldMap.isTransient()) {
          continue;
        }

        // Get the JsonNode first
        JsonNode valueNode = node.get(fieldMap.getName());
        Object fieldValue;
        if (valueNode == null) {
          // the value has not been provided, use the default value
          fieldValue = fieldMap.getDefaultValue();
        } else {
          if (fieldMap.getType().isReference()) {
            ResourceMap referenced = (ResourceMap) fieldMap.getType();
            // in case of reference, we might need to do a recursive insert
            if (valueNode.isObject()) {
              fieldValue = insertOrUpdateReference(referenced, (ObjectNode)valueNode);
            } else {
              fieldValue = referenced.getPrimaryField().getType().fromJson(valueNode);
            }
          } else {
            fieldValue = fieldMap.getType().fromJson(valueNode);
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
          Object id = map.getPrimaryField().getType().fromJDBC(keys, 1);
          node.putPOJO(map.getPrimaryField().getName(), map.getPrimaryField().getType().toJson(id));
          checkAndUpdate(manyList);
          return id;
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
  public Object update(ResourceMap map, ObjectNode node) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("UPDATE ");
    sqlBuilder.append(quoteSystemIdentifier(map.getName()));
    sqlBuilder.append(" SET");

    boolean first = true;

    FieldMap fields[] = new FieldMap[map.getFieldsCount()];
    JsonNode values[] = new JsonNode[map.getFieldsCount()];
    JsonNode primaryFieldValue = null;
    int valuesCount = 0;

    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap = map.getFieldMap(i);

      if (fieldMap.getType().isMany() || fieldMap.isTransient() || !node.has(fieldMap.getName())) {
        // Skip the Many relationship, transients and the ones that are not
        // available in the node
        continue;
      }

      if (fieldMap == map.getPrimaryField()) {
        primaryFieldValue = node.get(fieldMap.getName());
        if (primaryFieldValue == null || primaryFieldValue.isNull()) {
          throw new StoreException(map.getName() + " UPDATE request without ID value");
        }
        // no need to add the primary key field as well
        continue;
      }

      if (!first) {
        sqlBuilder.append(',');
      } else {
        first = false;
      }
      sqlBuilder.append(fieldMap.getName());
      sqlBuilder.append("=?");

      fields[valuesCount] = fieldMap;
      values[valuesCount] = node.get(fieldMap.getName());
      valuesCount += 1;
    }

    sqlBuilder.append(" WHERE ");
    sqlBuilder.append(map.getPrimaryField().getName());
    sqlBuilder.append("=?");

    if(primaryFieldValue == null) {
      throw new StoreException("Could not find primary field in " + map.getName());
    }

    try (PreparedStatement stmt = connection.prepareStatement(sqlBuilder.toString())) {
      // fill up all the '?';
      for(int i=0; i<valuesCount; ++i) {
        FieldMap fieldMap = fields[i];
        JsonNode nodeValue = values[i];

        if (nodeValue == null || nodeValue.isNull()) {
          stmt.setObject(i+1, null);
        } else {
          Object fieldValue;
          if (fieldMap.getType().isReference() && nodeValue.isObject()) {
            fieldValue = insertOrUpdateReference((ResourceMap) fieldMap.getType(), (ObjectNode) nodeValue);
          } else {
            fieldValue = fieldMap.getType().fromJson(nodeValue);
          }

          fieldMap.getType().toJDBC(stmt, (i+1), fieldValue);
        }
      }

      // Set the primary key value
      Object pKey = map.getPrimaryField().getType().fromJson(primaryFieldValue);
      map.getPrimaryField().getType().toJDBC(stmt, valuesCount+1, pKey);

      try {
        stmt.executeUpdate();
      } catch(SQLException e) {
        throw new StoreException("Error while executing sql - " + sqlBuilder.toString(), e);
      }

      return pKey;

    } catch(SQLException e) {
      throw new StoreException("Error while preparing sql - " + sqlBuilder.toString(), e);
    }
  }

  private void checkAndUpdate(List<Many> manyList) {
    for(Many item:manyList) {

    }
  }

  @Override
  public Object delete(ResourceMap map, Object id)
          throws StoreException {
    String sql = "DELETE FROM " + quoteSystemIdentifier(map.getName()) +
            "WHERE " + quoteSystemIdentifier(map.getPrimaryField().getName()) + "=?";

    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sql);
    } catch (SQLException e) {
      throw new StoreException("Error while preparing sql - " + sql, e);
    }
    map.getPrimaryField().getType().toJDBC(stmt, 1, id);

    int affectedRows = 0;
    try {
      affectedRows = stmt.executeUpdate();
    } catch (SQLException e) {
      throw new StoreException("Error while executing sql - " + sql, e);
    }

    if (affectedRows != 1) {
      throw new StoreException("Delete failed. " + affectedRows + " records deleted from " + map.getName() + " while trying to delete " + id);
    }

    return id;
  }
}
