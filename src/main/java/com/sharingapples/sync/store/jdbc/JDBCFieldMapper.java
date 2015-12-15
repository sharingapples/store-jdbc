package com.sharingapples.sync.store.jdbc;

import java.sql.*;
import java.time.*;

/**
 * A Field Mapper for for making the generic data types compatible with the
 * JDBC PreparedStatement and ResultSet
 *
 * Created by ranjan on 12/15/15.
 */
interface JDBCFieldMapper<T> {


  void setValue(PreparedStatement statement, int index, T value) throws SQLException;
  T getValue(ResultSet resultSet, int index) throws SQLException;

  JDBCFieldMapper<Integer> INT = new JDBCFieldMapper<Integer>() {
    @Override
    public void setValue(PreparedStatement statement, int index, Integer value) throws SQLException {
      if (value == null) {
        statement.setNull(index, Types.INTEGER);
      } else {
        statement.setInt(index, value);
      }
    }

    @Override
    public Integer getValue(ResultSet resultSet, int index) throws SQLException {
      int v = resultSet.getInt(index);
      return resultSet.wasNull() ? null : v;
    }
  };

  JDBCFieldMapper<String> STRING = new JDBCFieldMapper<String>() {
    @Override
    public void setValue(PreparedStatement statement, int index, String value) throws SQLException {
      if (value == null) {
        statement.setNull(index, Types.VARCHAR);
      } else {
        statement.setString(index, value);
      }
    }

    @Override
    public String getValue(ResultSet resultSet, int index) throws SQLException {
      return resultSet.getString(index);
    }
  };

  JDBCFieldMapper<LocalDate> DATE = new JDBCFieldMapper<LocalDate>() {
    @Override
    public void setValue(PreparedStatement statement, int index, LocalDate value) throws SQLException {
      if(value == null) {
        statement.setNull(index, Types.DATE);
      } else {
        statement.setDate(index, new Date(value.atStartOfDay(StoreJDBC.SYSTEM_TZ).toInstant().toEpochMilli()));
      }
    }

    @Override
    public LocalDate getValue(ResultSet resultSet, int index) throws SQLException {
      Date date = resultSet.getDate(index);
      if (date == null) {
        return null;
      } else {
        return Instant.ofEpochMilli(date.getTime()).atZone(StoreJDBC.SYSTEM_TZ).toLocalDate();
      }
    }
  };

  JDBCFieldMapper<LocalTime> TIME = new JDBCFieldMapper<LocalTime>() {
    @Override
    public void setValue(PreparedStatement statement, int index, LocalTime value) throws SQLException {
      if (value == null) {
        statement.setNull(index, Types.TIME);
      } else {
        statement.setTime(index, new Time(value.toSecondOfDay() * 1000));
      }
    }

    @Override
    public LocalTime getValue(ResultSet resultSet, int index) throws SQLException {
      Time time = resultSet.getTime(index);
      if (time == null) {
        return null;
      } else {
        return Instant.ofEpochMilli(time.getTime()).atZone(StoreJDBC.SYSTEM_TZ).toLocalTime();
      }
    }
  };

  JDBCFieldMapper<LocalDateTime> DATETIME = new JDBCFieldMapper<LocalDateTime>() {
    @Override
    public void setValue(PreparedStatement statement, int index, LocalDateTime value) throws SQLException {
      if (value == null) {
        statement.setNull(index, Types.TIMESTAMP);
      } else {
        statement.setTimestamp(index, new Timestamp(value.atZone(StoreJDBC.SYSTEM_TZ).toInstant().toEpochMilli()));
      }
    }

    @Override
    public LocalDateTime getValue(ResultSet resultSet, int index) throws SQLException {
      Timestamp timestamp = resultSet.getTimestamp(index);
      if (timestamp == null) {
        return null;
      } else {
        return Instant.ofEpochMilli(timestamp.getTime()).atZone(StoreJDBC.SYSTEM_TZ).toLocalDateTime();
      }
    }
  };
}
