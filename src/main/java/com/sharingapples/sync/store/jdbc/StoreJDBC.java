package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.store.Engine;
import com.sharingapples.sync.store.Store;
import com.sharingapples.sync.store.StoreException;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by ranjan on 12/12/15.
 */
public class StoreJDBC extends Store {

  private DataSource dataSource;

  public StoreJDBC(String driverClass, String connUrl, Properties connProps) throws StoreException {
    super();

    try {
      Class.forName(driverClass);
    } catch(ClassNotFoundException e) {
      throw new StoreException("The JDBC driver class - " + driverClass + " was not found", e);
    }

    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connUrl, connProps);

    PoolableConnectionFactory poolableConnectionFactory =
            new PoolableConnectionFactory(connectionFactory, null);
    ObjectPool<PoolableConnection> connectionPool = 
            new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);
    
    dataSource = new PoolingDataSource<>(connectionPool);
  }

  @Override
  protected Engine startEngine() {
    try {
      return new EngineJDBC(dataSource.getConnection());
    } catch(SQLException e) {
      throw new StoreException("Could not start JDBC Engine", e);
    }
  }



}
