package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.Registrar;
import com.sharingapples.sync.store.StoreException;

import java.util.Properties;

/**
 * Created by ranjan on 12/13/15.
 */
public class StorePostgres extends StoreJDBC {
  public StorePostgres(Registrar registrar, String host, int port, String database,
                       final String username, final String password)
          throws StoreException {
    super(registrar, "org.postgresql.Driver",
            "jdbc:postgresql://" + host + ":" + port + "/" + database,
            new Properties() {{
              this.setProperty("user", username);
              this.setProperty("password", password);
            }}
    );
  }
}
