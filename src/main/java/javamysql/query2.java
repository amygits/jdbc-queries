package javamysql;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

public class query2 {
    
    public static void main (String[] args) {
	
	ResultSet result = null; 
	PreparedStatement statement = null;
	Connection connection = null;
	
	if (args.length < 1) {
	    System.out.println("Usage java javamysql.src.main.java.query2 <url> <user> <password> <driver> <dept no>");
	    System.exit(0);
	}
	String url = args[0];
	
	try {
	    Class.forName(args[3]);
	    
	    connection = DriverManager.getConnection(url, args[1], args[2]);
	    
	    statement = connection.prepareStatement("SELECT DNAME, NAME, PRICE\r\n"
	    	+ "FROM product, dept, customer\r\n"
	    	+ "WHERE product.MADE_BY = dept.DEPTNO \r\n"
	    	+ "AND customer.PID = product.PRODID\r\n"
	    	+ "AND dept.DEPTNO = ?;");
	    statement.setString(1, args[4]);
	    
	    result = statement.executeQuery();
	    System.out.println("Dept | Customer | Price\n-----------------------");
	    while (result.next()) {
        	String price = result.getString("PRICE");
        	String name = result.getString("NAME");
        	String dname = result.getString("DNAME");
        	System.out.println(dname + " | " + name + " | " + price);
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
