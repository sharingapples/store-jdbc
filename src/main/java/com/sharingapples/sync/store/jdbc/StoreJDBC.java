package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.DataType;
import com.sharingapples.sync.resource.Registrar;
import com.sharingapples.sync.resource.ResourceMap;
import com.sharingapples.sync.store.Engine;
import com.sharingapples.sync.store.Store;
import com.sharingapples.sync.store.StoreException;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ranjan on 12/12/15.
 */
public class StoreJDBC extends Store {
  public static ZoneId SYSTEM_TZ = ZoneId.of("UTC");

  private DataSource dataSource;
  private Map<DataType, JDBCFieldMapper> mappedJDBCFieldTypes = new HashMap<>();

  public StoreJDBC(Registrar registrar, String driverClass, String connUrl, Properties connProps) throws StoreException {
    super(registrar);

    // Initialize the driver
    try {
      Class.forName(driverClass);
    } catch(ClassNotFoundException e) {
      throw new StoreException("The JDBC driver class - " + driverClass + " was not found", e);
    }


    // Create Connection Pool Using Apache DBCP2
    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connUrl, connProps);
    PoolableConnectionFactory poolableConnectionFactory =
            new PoolableConnectionFactory(connectionFactory, null);
    ObjectPool<PoolableConnection> connectionPool = 
            new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    dataSource = new PoolingDataSource<>(connectionPool);


    // Register all the field mappers as well
    mappedJDBCFieldTypes.put(DataType.INT, JDBCFieldMapper.INT);
    mappedJDBCFieldTypes.put(DataType.STRING, JDBCFieldMapper.STRING);
    mappedJDBCFieldTypes.put(DataType.DATE, JDBCFieldMapper.DATE);

  }

  public JDBCFieldMapper getJDBCFieldType(DataType type) {
    if (type.isReference()) {
      return getJDBCFieldType(((ResourceMap)type).getPrimaryField().getType());
    } else if (type.isMany()) {
      throw new StoreException("Many Relations cannot be mapped into JDBC queries get/set");
    } else {
      return mappedJDBCFieldTypes.get(type);
    }
  }

  @Override
  protected Engine startEngine() {
    try {
      return new EngineJDBC(this, dataSource.getConnection());
    } catch(SQLException e) {
      throw new StoreException("Could not start JDBC Engine", e);
    }
  }



}
