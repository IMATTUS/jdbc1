package application;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import db.DB;
import db.DbException;
import db.DbIntegrityException;

public class Main {

	public static void main(String[] args) {
//		runSelectSQL();
//		runInsertSQLPreparedStatement();
//		runInsertSQLPreparedStatementReturningID();
//		runUpdateSQL();
//		runDeleteSQL();
		runJdbc();
		System.out.println("Bye!");
		
	}
	
	public static void runJdbc() {
		/*
		 * Transações com JDBC 
		 * Usamos basicamente 3 elementos
		 * setAutoCommit - se verdadeiro, cada operação é comitada automaticamente
		 * commit - confirma a transação
		 * rollback() - desfazemos as alterações.
		 */
		
		Connection conn = null;
		Statement st = null;
		try {
			
			conn = DB.getConnection();
			conn.setAutoCommit(false);
			st = conn.createStatement();
			
			
			int rows1 = st.executeUpdate(""
					+ "UPDATE seller set BaseSalary = 2090 "
					+ "WHERE DepartmentId = 1");
			
//			int x = 1;
//			if(x<2) {
//				throw new SQLException("Fake Error ");
//			}
			
			int rows2 = st.executeUpdate(""
					+ "UPDATE seller set BaseSalary = 3090 "
					+ "WHERE DepartmentId = 2");
			
			conn.commit();
			
			System.out.println("Done! rows1: " + rows1);
			System.out.println("Done! rows2: " + rows2);
					
		}catch(SQLException e) {
			try {
				conn.rollback();
				throw new DbException("Transaction rolled back, caused by " + e.getMessage());
			} catch (SQLException e1) {
				throw new DbException("Error trying to rollback " + e1.getMessage());
			}
		}finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
		
	}
	
	public static void runDeleteSQL() {
		Connection conn = null;
		PreparedStatement st = null;
		try {
			
			conn = DB.getConnection();
			st = conn.prepareStatement(""
					+ "DELETE FROM department "
					+ "WHERE "
					+ "(ID = ?) ");
			st.setInt(1, 2);		
			int rowsAffacted = st.executeUpdate();
			
			System.out.println("Done! Rows affected: "+ rowsAffacted);
					
		}catch(SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		}finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
		
	}
	
	public static void runUpdateSQL() {
		Connection conn = null;
		PreparedStatement st = null;
		
		try {
			
			conn = DB.getConnection();
			st = conn.prepareStatement(""
					+ "UPDATE seller "
					+ "SET BaseSalary = BaseSalary + ? "
					+ "WHERE "
					+ "(DepartmentId = ?) ");
			st.setDouble(1, 200.0);		
			st.setInt(2, 2);		
			int rowsAffacted = st.executeUpdate();
			
			System.out.println("Done! Rows affected: "+ rowsAffacted);
					
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}
	
	public static void runInsertSQLPreparedStatementReturningID() {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Connection conn = null;
		PreparedStatement st = null;
		try {
			conn = DB.getConnection();
//			st = conn.prepareStatement(""
//					+ "INSERT INTO seller "
//					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
//					+ "VALUES "
//					+ "(?,?,?,?,?)",
//					Statement.RETURN_GENERATED_KEYS);
//			
//			st.setString(1, "Carl Purple");
//			st.setString(2, "Carl@gmail.com");
//			st.setDate(3, new java.sql.Date(sdf.parse("22/04/1985").getTime()));
//			st.setDouble(4, 3000.0);
//			st.setInt(5, 4);
			
			st = conn.prepareStatement("insert into department (name)"
					+ "values ('D1'), ('D2')",
					Statement.RETURN_GENERATED_KEYS);
			
			int rowsAffected = st.executeUpdate();
			if(rowsAffected>0) {
				ResultSet rs = st.getGeneratedKeys();
				
				while(rs.next()) {
					int id = rs.getInt(1);
					System.out.println("Done! ID = " + id);
				}
				DB.closeResultSet(rs);
				
			}else {
				System.out.println("No rows affected");
			}
			
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}
	
	public static void runInsertSQLPreparedStatement() {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Connection conn = null;
		PreparedStatement st = null;
		try {
			conn = DB.getConnection();
			st = conn.prepareStatement(""
					+ "INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES "
					+ "(?,?,?,?,?)");
			
			st.setString(1, "Carl Purple");
			st.setString(2, "Carl@gmail.com");
			st.setDate(3, new java.sql.Date(sdf.parse("22/04/1985").getTime()));
			st.setDouble(4, 3000.0);
			st.setInt(5, 4);
			
			int rowsAffected = st.executeUpdate();
			System.out.println("Done! Rows affected: " + rowsAffected);
			
		}catch(SQLException e) {
			e.printStackTrace();
		}catch(ParseException e) {
			e.printStackTrace();
		}finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}
	
	public static void runSelectSQL() {
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		try {
			conn = DB.getConnection(); 
			st = conn.createStatement();
			rs=st.executeQuery("Select * from department");
			
			while(rs.next()) {
				System.out.println(rs.getInt("Id") +", "+rs.getString("Name"));
			}
			
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}

}
