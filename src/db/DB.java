package db;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DB {

	/*
	 * the connection to the database
	 */
	private static Connection conn = null;

	/*
	 * will return the connection to the database it will also connect to the
	 * database in case that the connection object is null
	 */
	public static Connection getConnection() {
		if (conn == null) {
			try {
				Properties props = loadProperties();
				String url = props.getProperty("dburl");
				conn = DriverManager.getConnection(url, props);
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
		return conn;
	}

	public static void closeConnection() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}

	/*
	 * Will read the properties from the db.properties file the method will load it
	 * and return an object Properties with the information
	 */
	private static Properties loadProperties() {

		try (FileInputStream fs = new FileInputStream("db.properties")) {
			Properties props = new Properties();
			props.load(fs);
			return props;
		} catch (IOException e) {
			throw new DbException(e.getMessage());
		}

	}
	
	public static void closeStatement(Statement st) {
		if(st!=null) {
			try {
				st.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
	
	public static void closeResultSet(ResultSet rs) {
		if(rs!=null) {
			try {
				rs.close();
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
}
