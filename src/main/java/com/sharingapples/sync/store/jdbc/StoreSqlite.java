package com.sharingapples.sync.store.jdbc;

import com.sharingapples.sync.resource.Registrar;
import com.sharingapples.sync.store.StoreException;

import java.io.File;
import java.sql.*;
import java.util.Properties;

/**
 * Created by ranjan on 12/13/15.
 */
public class StoreSqlite extends StoreJDBC {
  public StoreSqlite(Registrar registrar, File dbFile) throws StoreException {
    super(registrar, "org.sqlite.JDBC",
            "jdbc:sqlite:" + dbFile.getAbsolutePath(), new Properties());
  }


  @Override
  public String getVersion() {
    try(EngineJDBC engine = this.startEngine()) {
      Statement stmt = engine.getConnection().createStatement();
      ResultSet rs = stmt.executeQuery("PRAGMA user_version");
      if (rs.next()) {
        return Integer.toString(rs.getInt(1));
      } else {
        return "0";
      }
    } catch(SQLException e) {
      throw new StoreException("Error while retrieving sqlite version", e);
    }
  }

  @Override
  public void setVersion(String version) {
    try(EngineJDBC engine = this.startEngine()) {
      PreparedStatement stmt = engine.getConnection().prepareStatement(
              "PRAGMA user_version=?");
      stmt.setInt(1, Integer.parseInt(version));
      stmt.execute();
    } catch(NumberFormatException e) {
      throw new StoreException("Sqlite supports only integer versioning");
    } catch(SQLException e) {
      throw new StoreException("Error while setting sqlite version", e);
    }
  }
}
