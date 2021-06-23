package javamysql;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;



public class JdbcLab {

    public static void main (String[] args) throws ParserConfigurationException, TransformerException{

	ResultSet result = null;
	Statement statement = null;
	Connection connection = null; 
	PreparedStatement prestatement = null;

	String q1 = "query1";
	String q2 = "query2";
	String q3 = "dml1";
	String q4 = "export";
	
	if (args.length < 4) {
	    System.out.println("Usage: java javamysql.JdbcLab.java <url> <user> <password> <driver> <query>"
		    + "\nAvailable query options: "
		    + "{<query1>, <query2>, <dml1>, <export>}");
	    System.exit(0);
	}

	if (args[4].equals(q1)) {
	    query1(result, statement, connection, args);
	}
	else if (args[4].equals(q2)) {
	    query2(result, prestatement, connection, args);
	}
	else if (args[4].equals(q3)) {
	    query3(result, prestatement, statement, connection, args);
	}
	else if (args[4].equals(q4)) {
	    query4(result, statement, connection, args);
	}
	else {
	    System.out.println("Usage: java javamysql.JdbcLab.java <url> <user> <password> <driver> <query>"
		    + "\nAvailable query options: "
		    + "{<query1>, <query2>, <dml1>, <export>}");
	    System.exit(0);
	}

    }


    private static void query1(ResultSet result, Statement statement, Connection connection, String[] args) {

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

    private static void query2(ResultSet result, PreparedStatement statement, Connection connection, String[] args) {

	if (args.length != 6) {
	    System.out.println("Usage java javamysql.src.main.java.query2 <url> <user> <password> <driver> query2 <dept no>");
	    System.exit(0);
	}

	try {
	    String url = args[0];
	    Class.forName(args[3]);

	    connection = DriverManager.getConnection(url, args[1], args[2]);
	    connection.setAutoCommit(false);
	    System.out.println("Connection successful");

	    statement = connection.prepareStatement("SELECT DNAME, NAME, PRICE\r\n"
		    + "FROM product, dept, customer\r\n"
		    + "WHERE product.MADE_BY = dept.DEPTNO \r\n"
		    + "AND customer.PID = product.PRODID\r\n"
		    + "AND dept.DEPTNO = ?;");
	    statement.setString(1, args[5]);

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


    private static void query3(ResultSet result, PreparedStatement prestatement, Statement statement, Connection connection, String[] args) {

        if (args.length != 9) {
    	System.out.println("Usage java javamysql.src.main.java.dml1 <url> <user> <password> <driver> dml1 <customer id> <product id> <name> <quantity>");
    	System.exit(0);
        }
        
	try {
	    String url = args[0];

	    Class.forName(args[3]);

	    connection = DriverManager.getConnection(url, args[1], args[2]);
	    connection.setAutoCommit(false);
	    System.out.println("Connection successful");

	    prestatement = connection.prepareStatement("INSERT INTO customer VALUES (?, ?, ?, ?)");
	    prestatement.setString(1, args[5]);
	    prestatement.setString(2, args[6]);
	    prestatement.setString(3, args[7]);
	    prestatement.setString(4, args[8]);
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


    private static void query4(ResultSet result, Statement statement, Connection connection, String[] args) {

	// if no filename is specified 
	if (args.length != 6) {
	    System.out.println("Usage java javamysql.src.main.java.dml1 <url> <user> <pwd> <driver> export <filename>");
	    System.exit(0);
	}

	try {

	    String url = args[0];
	    Class.forName(args[3]);
	    String filename = args[5];

	    connection = DriverManager.getConnection(url, args[1], args[2]);
	    connection.setAutoCommit(false);
	    System.out.println("Connection successful");
	    
	    // Creating document builder instances
	    DocumentBuilderFactory docFactory= DocumentBuilderFactory.newInstance();
	    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

	    // create new doc for each table
	    Document customerdoc = docBuilder.newDocument();
	    Document empdoc = docBuilder.newDocument();
	    Document proddoc = docBuilder.newDocument();
	    Document deptdoc = docBuilder.newDocument();

	    // Root Elements specific to each doc
	    Element customer = customerdoc.createElement("customer");
	    Element employee = empdoc.createElement("employee");
	    Element dept = deptdoc.createElement("department");
	    Element product = proddoc.createElement("product");

	    // Appending roots to each doc 
	    customerdoc.appendChild(customer);
	    empdoc.appendChild(employee);
	    deptdoc.appendChild(dept);
	    proddoc.appendChild(product);	

	    // first query for customers
	    String customerquery = "SELECT *\n"
		    + "FROM CUSTOMER";
	    statement = connection.createStatement();
	    result = statement.executeQuery(customerquery);
	    /* while there are results
	      elements will be added */
	    while (result.next()) {
		String cid = result.getString("CUSTID");
		String prodid = result.getString("PID");
		String cname = result.getString("NAME");
		String quan = result.getString("QUANTITY");

		customer.appendChild(getCustomer(customerdoc, cid, prodid, cname, quan));
	    }

	    // second query for employees
	    String empquery = "SELECT *\r"
		    + "FROM EMP;";
	    result = statement.executeQuery(empquery);
	    while (result.next()) {
		String eno = result.getString("EMPNO");
		String ename = result.getString("ENAME");
		String role = result.getString("JOB");
		String mgr = result.getString("MGR");
		String salary = result.getString("SAL");
		String comm = result.getString("COMM");
		String dno = result.getString("DEPTNO");

		employee.appendChild(getEmp(empdoc, eno, ename, role, mgr, salary, comm, dno));
	    }

	    // third query for dept
	    String deptquery = "SELECT *\r\n"
		    + "FROM dept;";
	    result = statement.executeQuery(deptquery);
	    while (result.next()) {
		String dno = result.getString("DEPTNO");
		String dname = result.getString("DNAME");
		String loc = result.getString("LOC");

		dept.appendChild(getDept(deptdoc, dno, dname, loc));
	    }

	    // fourth query for product
	    String prodquery = "SELECT *\n FROM product;";
	    result = statement.executeQuery(prodquery);
	    while (result.next()) {
		String pid = result.getString("PRODID");
		String price = result.getString("PRICE");
		String madeby = result.getString("MADE_BY");
		String desc = result .getString("DESCRIP");

		product.appendChild(getProd(proddoc, pid, price, madeby, desc));
	    }

	    // Output to file/Console
	    TransformerFactory transformerFactory = TransformerFactory.newInstance();
	    Transformer transformer = transformerFactory.newTransformer();
	    // Pretty print
	    transformer.setOutputProperty(OutputKeys.INDENT, "yes");

	    DOMSource custsource = new DOMSource(customerdoc);
	    DOMSource deptsource = new DOMSource(deptdoc);
	    DOMSource prodsource = new DOMSource(proddoc);
	    DOMSource empsource = new DOMSource(empdoc);

	    // Writes to console
	    /*StreamResult console = new StreamResult(System.out);
	    transformer.transform(custsource, console);
	    transformer.transform(deptsource, console);
	    transformer.transform(prodsource, console);
	    transformer.transform(empsource, console);*/

	    // Writes to file
	    StreamResult file1 = new StreamResult(new File(filename + "-customer.xml"));
	    transformer.transform(custsource, file1);
	    StreamResult file2 = new StreamResult(new File(filename + "-dept.xml"));
	    transformer.transform(deptsource, file2);
	    StreamResult file3 = new StreamResult(new File(filename + "-prod.xml"));
	    transformer.transform(prodsource, file3);
	    StreamResult file4 = new StreamResult(new File(filename + "-emp.xml"));
	    transformer.transform(empsource, file4);
	    System.out.println("Done");

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

    private static Node getCustomer(Document doc, String custid, String pid, String name, String quan) {
	Element customer = doc.createElement("Customer");
	// set attribute
	customer.setAttribute("custid", custid);
	// creates name element
	customer.appendChild(getElements(doc, customer, "name",name));
	// creates pid attribute
	customer.appendChild(getElements(doc, customer, "pid", pid));
	// creates quantity attribute
	customer.appendChild(getElements(doc, customer, "cpid", pid));

	return customer;
    }

    private static Node getEmp(Document doc, String empno, String ename, String job, String mgr, String sal, String comm, String deptno) {
	Element employee = doc.createElement("Employee");
	// set attribute
	employee.setAttribute("employeenum", empno);
	// create elements
	employee.appendChild(getElements(doc, employee, "name",empno));
	employee.appendChild(getElements(doc, employee, "pid", empno));
	employee.appendChild(getElements(doc, employee, "role", job));
	employee.appendChild(getElements(doc, employee, "manager", mgr));
	employee.appendChild(getElements(doc, employee, "salary", sal));
	employee.appendChild(getElements(doc, employee, "comm", comm));
	employee.appendChild(getElements(doc, employee, "dept-num", deptno));

	return employee;
    }

    private static Node getDept(Document doc, String deptno, String dname, String loc) {
	Element dept = doc.createElement("Department");
	// set attribute
	dept.setAttribute("deptno", deptno);
	// create elements
	dept.appendChild(getElements(doc, dept, "deptname", dname));
	dept.appendChild(getElements(doc, dept, "location", loc));

	return dept;
    }

    private static Node getProd(Document doc, String prodid, String price, String madeby, String descrip) {
	Element prod = doc.createElement("Product");
	// set attribute
	prod.setAttribute("productid", prodid);
	// create elements
	prod.appendChild(getElements(doc, prod, "price", price));
	prod.appendChild(getElements(doc, prod, "madeby", madeby));
	prod.appendChild(getElements(doc, prod, "description", descrip));

	return prod;
    }   

    public static Node getElements(Document doc, Element element, String name, String value) {
	Element node = doc.createElement(name);
	node.appendChild(doc.createTextNode(value));
	return node;
    }

}
