package javamysql;
import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

public class JdbcLab2 {

    public static void main (String[] args) throws ParserConfigurationException, TransformerException{ {

	if (args.length != 1) {
	    System.out.println("Usage java src.javamysql.JdbcLab2 <DeptNo>");
	    System.exit(0);
	}
	String deptno = args[0];
	DocumentBuilderFactory builderFactory = 
		DocumentBuilderFactory.newInstance();
	DocumentBuilder builder = null; 


	try {
	    builder = builderFactory.newDocumentBuilder();

	    Document proddoc = builder.parse( new FileInputStream("export-prod.xml"));
	    XPath xPath = XPathFactory.newInstance().newXPath();
	    String query = "//madeby[text()='" + deptno + "']/ancestor::*";
	   // System.out.println(query);
	    
	   NodeList nodeList = (NodeList) xPath.compile(query).evaluate(proddoc, XPathConstants.NODESET);
	   for (int i = 1; i < nodeList.getLength(); i++) {
	        NodeList nodList = nodeList.item(i).getChildNodes();
	        for (int j = 0; j < nodList.getLength(); j++) {
	            Node nod = nodList.item(j);
	            if (nod.getNodeType() == Node.ELEMENT_NODE && nodList.item(j).getNodeName().equals("description")) {
	        	System.out.println(nodList.item(j).getNodeName() + ": " + nod.getFirstChild().getNodeValue());
	            }
	        }
	    }

	} catch (ParserConfigurationException e) {
	    e.printStackTrace();  
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (SAXException e) {
	    e.printStackTrace();
	} catch (XPathExpressionException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }
    }

}
