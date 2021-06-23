

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class query1 {
    
    public static void main(String[] args) {
	
	ResultSet result = null;
	Statement statement = null;
	Connection connection = null; 
	
	if (args.length < 1) {
	    System.out.println("Usage: java net.codejava.query1 <url> <user> <password> <driver>");
	    System.exit(0);
	}
     
        try {
            String url = args[0];
            Class.forName(args[3]);
            connection = DriverManager.getConnection(url, args[1], args[2]);
            connection.setAutoCommit(false);
            System.out.println("Connection successful");
            
            String sql = "SELECT ENAME, EMPNO, DNAME FROM dept, emp WHERE dept.DEPTNO = emp.DEPTNO";
            statement = connection.createStatement();
            result = statement.executeQuery(sql);
            
            while (result.next()) {
        	String eID = result.getString("EMPNO");
        	String name = result.getString("ENAME");
        	String dname = result.getString("DNAME");
        	System.out.println("NAME:" + name + "\tID:" + eID + "\tDEPT:" + dname);
            }
            
        } catch (SQLException throwables) {
            System.out.println("Ran into a connection error!");
            throwables.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally { // Close connections
            try {
        	if (result != null) {
        	    result.close();
        	}
        	if (statement != null) {
        	    statement.close();
        	}
            } catch (Throwable t) {
        	System.out.println("Problem closing DB resoures");
            }
            try {
        	if (connection != null) {
        	    connection.close();
        	} 
            } catch (Throwable t2) {
        	System.out.println("Connection leak warning!");
            }
        }

    }
}
