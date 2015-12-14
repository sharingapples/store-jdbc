package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.FieldMap;
import com.sharingapples.sync.resource.ResourceMap;
import com.sharingapples.sync.resource.ResourceMarker;
import com.sharingapples.sync.store.RecordSet;
import com.sharingapples.sync.store.StoreException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by ranjan on 12/13/15.
 */
public class JDBCRecordSet<T extends ResourceMarker> implements RecordSet<T> {

  private final Statement statement;
  private final ResourceMap<T> map;
  private final ResultSet rs;


  private final int[] columnIndexes;
  private final int primaryFieldColumnIndex;

  JDBCRecordSet(Statement statement, ResourceMap<T> map, ResultSet rs) throws StoreException {
    this.statement = statement;
    this.map = map;
    this.rs = rs;

    columnIndexes = new int[map.getFieldsCount()];
    int primaryFieldColumnIndex = 0;
    for(int i=0; i<map.getFieldsCount(); ++i) {
      FieldMap fieldMap = map.getFieldMap(i);

      // skip the many relations field
      if (fieldMap.getType().isMany()) {
        continue;
      }

      try {
        columnIndexes[i] = rs.findColumn(fieldMap.getName());
      } catch(SQLException e) {
        throw new StoreException("Could not find field " + fieldMap.getName() + " in the result set of " + map.getName() + ". Check database structure.", e);
      }
      if (map.getPrimaryField() == fieldMap) {
        primaryFieldColumnIndex = columnIndexes[i];
      }
    }

    if (primaryFieldColumnIndex == 0) {
      throw new StoreException("Unexpected error. The primary key field was not found for " + map.getName() + ".");
    }

    this.primaryFieldColumnIndex = primaryFieldColumnIndex;
  }

  @Override
  public boolean hasNext() {
    try {
      if (rs.next()) {
        return true;
      } else {
        rs.close();
        statement.close();
        return false;
      }
    } catch(SQLException e) {
      throw new StoreException("Error while trying to move to next record", e);
    }
  }

  @Override
  public T next() {

    // First get the id of the record
    Object id = map.getPrimaryField().getType().fromJDBC(rs, primaryFieldColumnIndex);
    // if we have it in cache, we will update the same object, otherwise create a new one
    T res = map.find(id);
    for (int i = 0; i < map.getFieldsCount(); ++i) {
      FieldMap fieldMap = map.getFieldMap(i);

      // skip the many relations field
      if (fieldMap.getType().isMany()) {
        continue;
      }

      fieldMap.set(res, fieldMap.getType().fromJDBC(rs, columnIndexes[i]));
    }

    return res;

  }


}
