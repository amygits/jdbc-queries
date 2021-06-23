package javamysql;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;

import com.mysql.cj.jdbc.DatabaseMetaData;

public class dml1 {

    public static void main (String [] args) {
        ResultSet result = null; 
        PreparedStatement prestatement = null;
        Statement statement = null;
        Connection connection = null;
        
        if (args.length < 1) {
    	System.out.println("Usage java javamysql.src.main.java.dml1 <url> <user> <password> <driver> dml1 <customer id> <product id> <name> <quantity>");
    	System.exit(0);
        }
        
        try {
    	String url = args[0];
    	
    	Class.forName(args[3]);
    	
    	connection = DriverManager.getConnection(url, args[1], args[2]);
    	connection.setAutoCommit(false);
    	
    	DatabaseMetaData metadata = (DatabaseMetaData) connection.getMetaData();
    	
    	prestatement = connection.prepareStatement("INSERT INTO customer VALUES (?, ?, ?, ?)");
    	prestatement.setString(1, args[4]);
    	prestatement.setString(2, args[5]);
    	prestatement.setString(3, args[6]);
    	prestatement.setString(4, args[7]);
    	if (prestatement.executeUpdate() > 0) {
    	    System.out.println("Tuple successfully inserted.");
    	}
    	prestatement.close();
    	connection.commit();
    	
    	statement = connection.createStatement();
    	result = statement.executeQuery("Select * from customer");
    	while (result.next()) {
    	String custid = result.getString("CUSTID");
    	String name = result.getString("NAME");
    	String pid = result.getString("PID");
    	String quantity = result.getString("QUANTITY");
    	System.out.println(custid + " | " + name + " | " + pid + " | " + quantity);
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
