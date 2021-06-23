package javamysql;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.File;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class export {

    public static void main (String [] args) throws ParserConfigurationException, TransformerException{ 

	DocumentBuilderFactory docFactory= DocumentBuilderFactory.newInstance();
	DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

	// create new doc for each table
	Document customerdoc = docBuilder.newDocument();
	Document empdoc = docBuilder.newDocument();
	Document proddoc = docBuilder.newDocument();
	Document deptdoc = docBuilder.newDocument();

	// Root Elements for each doc
	Element customer = customerdoc.createElement("customer");
	Element employee = empdoc.createElement("employee");
	Element dept = deptdoc.createElement("department");
	Element product = proddoc.createElement("product");
	
	// Appending roots to each doc 
	customerdoc.appendChild(customer);
	empdoc.appendChild(employee);
	deptdoc.appendChild(dept);
	proddoc.appendChild(product);

	// initializing connection variables
	ResultSet result = null; 
	Statement statement = null;
	Connection connection = null;

	// if arg length is less than 1
	if (args.length < 1) {
	    System.out.println("Usage java javamysql.src.main.java.dml1 <url> <user> <pwd> <driver> export <filename>");
	    System.exit(0);
	}

	try {
	    String url = args[0];

	    Class.forName(args[3]);
	    String filename = args[4];

	    connection = DriverManager.getConnection(url, args[1], args[2]);
	    connection.setAutoCommit(false);

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
	    StreamResult file = new StreamResult(new File(filename + ".xml"));
	    transformer.transform(custsource, file);
	    transformer.transform(deptsource, file);
	    transformer.transform(prodsource, file);
	    transformer.transform(empsource, file);
	    
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
	Element prod = doc.createElement("Department");
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

