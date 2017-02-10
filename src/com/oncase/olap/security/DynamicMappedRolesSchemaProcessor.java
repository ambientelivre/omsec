package com.oncase.olap.security;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import pt.webdetails.di.baserver.utils.web.Response;
import pt.webdetails.di.baserver.utils.web.HttpConnectionHelper;
import mondrian.olap.Util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import com.oncase.olap.security.di.XMLWorker;
import com.oncase.olap.security.exception.DSPException;
import com.oncase.olap.security.exception.PropertiesLoadException;
import com.oncase.olap.security.exception.SchemaLoadException;

public class DynamicMappedRolesSchemaProcessor implements
		mondrian.spi.DynamicSchemaProcessor {

	final String PROPERTIES = "cubeguard.properties";
	final String PLUGIN_PATH = "system/cubeguard";

	private Properties prop;
	private boolean replceString;
	private boolean debug;
	private String stringToBeReplaced, defaultEndpointName,defaultEndpointType,
					endpointType,endpointName, sessionVarsToReplace;

	public DynamicMappedRolesSchemaProcessor() throws PropertiesLoadException {

		String propPath = getSolutionRootPath() + "/" + PLUGIN_PATH + "/"
				+ PROPERTIES;

		try {
			setProperties(propPath);
		} catch (IOException e) {
			throw new PropertiesLoadException("Error loading properties file");
		}

		replceString = "true".equals(prop.get("cubeguard.replaceString"));
		stringToBeReplaced = (String) prop.get("cubeguard.stringToBeReplaced");
		defaultEndpointName = (String) prop.get("cubeguard.defaultEndpointName");
		defaultEndpointType =  (String) prop.get("cubeguard.defaultEndpointType");
		debug =  "true".equals(prop.get("cubeguard.debug"));
		sessionVarsToReplace = (String) prop.get("cubeguard.sessionVarsToReplace");

	}

	@Override
	public String processSchema(String schemaUrl, Util.PropertyList connectInfo)
			throws DSPException, ParserConfigurationException, ParseException,
			TransformerException {

		String userName = getUserName();
		String schemaText = getXMLSchema(schemaUrl);
		
		String catalogName = connectInfo.get("Catalog");
		
		if(catalogName!=null){   
			catalogName = catalogName.substring(catalogName.indexOf(":/")+2);
		}else{
			catalogName = "NOCATALOGPROVIDED";
		}
		
		endpointName = connectInfo.get("EndpointName");
		endpointType = connectInfo.get("EndpointType");
		HashMap<String, String> sessionVars = getResolvedSessionVars();
		
		if(endpointName==null)
			endpointName = defaultEndpointName;
		
		if(endpointType==null)
			endpointType = defaultEndpointType;

		if (replceString)
			schemaText = schemaText.replace(stringToBeReplaced, userName);
		
		if(sessionVars != null && sessionVars.size() > 0){
			for (Map.Entry<String, String> entry : sessionVars.entrySet()) {
			    String key = entry.getKey();
			    String value = entry.getValue();
			    schemaText = schemaText.replace(key, value);
			}
		}

		schemaText = getModifiedSchema(schemaText, catalogName);
		printDebug("Schema text:", schemaText);
		printDebug("Catalog name", catalogName);
		printDebug("Connection info:", connectInfo);
		printDebug("Replace String?", replceString);
		printDebug("stringToBeReplaced:", stringToBeReplaced);
		printDebug("Current session Vars to be replaced:", sessionVars.toString());
		
		
		return schemaText;

	}
	
	private HashMap<String, String> getResolvedSessionVars(){
		
		if(sessionVarsToReplace == null || sessionVarsToReplace.trim()==""){
			printDebug("No session variables set", "Property cubeguard.sessionVarsToReplace is empty;");
			return null;
		}
		
		IPentahoSession session = PentahoSessionHolder.getSession();
		
		String[] rawVars = sessionVarsToReplace.split("\\s?,\\s?");
		HashMap<String, String> sessionVars = new HashMap<String, String>();
		
		for(int i = 0 ; i < rawVars.length ; i++){
			String currentSessionVar = rawVars[i];
			if(currentSessionVar == null)
				continue;
			
			currentSessionVar = currentSessionVar.trim();
			
			if(currentSessionVar.matches("^\\$\\{\\w+\\}$")){
				String varName = currentSessionVar.substring(2, currentSessionVar.length()-1);
				Object sessionValue = session.getAttribute(varName);
				if(sessionValue == null) sessionValue = new String("");
				sessionVars.put(currentSessionVar, sessionValue.toString());
			}
		}
		
		return sessionVars;
		
	}

	private String getModifiedSchema(String schemaText, String catalogName)
			throws ParserConfigurationException, ParseException,
			TransformerException {

		String rolesText = "resultset".equals(endpointType) 
				? getXMLRoles(getEndpointResultSet(catalogName))
						: getEndpointResultSet(catalogName);

		String modifiedSchema = rolesText.equals("") ? schemaText
				: replaceSchemaRoles(schemaText, rolesText);

		return modifiedSchema;

	}

	private String getXMLRoles(String endpointResultSet)
			throws ParserConfigurationException, ParseException,
			TransformerException {
		XMLWorker xml = new XMLWorker(getResultSet(endpointResultSet));
		return xml.getXML();

	}

	private ArrayList<Object[]> getResultSet(String endpointResultSet)
			throws ParseException {

		JSONParser parser = new JSONParser();
		ArrayList<Object[]> output = new ArrayList<Object[]>();

		Object obj = parser.parse(endpointResultSet);
		JSONObject json = (JSONObject) obj;

		JSONArray resultSet = (JSONArray) json.get("resultset");
		Iterator<?> it = resultSet.iterator();
		while (it.hasNext()) {
			JSONArray innerArray = (JSONArray) it.next();
			Object[] row = { (String) innerArray.get(XMLWorker.I_USER),
					(String) innerArray.get(XMLWorker.I_SCHEMA),
					(String) innerArray.get(XMLWorker.I_CUBE),
					(String) innerArray.get(XMLWorker.I_HIERARCHY),
					(String) innerArray.get(XMLWorker.I_MEMBER),
					(int) (long) innerArray.get(XMLWorker.I_TYPE) };

			output.add(row);

		}

		return output;
	}

	private String getSolutionRootPath() {
		return PentahoSystem.getApplicationContext().getSolutionRootPath();
	}

	private void setProperties(String filePath) throws IOException {

		prop = new Properties();
		InputStream inputStream = new FileInputStream(filePath);

		prop.load(inputStream);

	}

	private String replaceSchemaRoles(String schemaText, String rolesText) {

		String from = schemaText.contains("</VirtualCube>") ? "</VirtualCube>"
				: "</Cube>";

		String to = schemaText.contains("<UserDefinedFunction ") ? "<UserDefinedFunction "
				: schemaText.contains("<Parameter ") ? "<Parameter "
						: "</Schema>";

		Integer lenFrom = from.length();
		Integer posFrom = schemaText.lastIndexOf(from);
		Integer posTo = schemaText.indexOf(to);

		String prepend = (posFrom > 0) ? schemaText.substring(0, posFrom
				+ lenFrom) : "";

		String append = (posTo > 0) ? schemaText.substring(posTo) : schemaText;

		return (posFrom > 0 && posTo > 0) ? prepend + "\n\n" + rolesText
				+ "\n\n" + append : schemaText;

	}

	private String getEndpointResultSet(String catalogName) {

		String accessRules = "";
		Map<String, String> queryParameters = new HashMap<String, String>();
		final String kettleOutput = "resultset".equals(endpointType) ? "Json" : "SingleCell";
		queryParameters.put("kettleOutput", kettleOutput);
		queryParameters.put("paramcatalogName", catalogName);

		Response response = HttpConnectionHelper.invokeEndpoint("cubeguard",
				"/"+endpointName, "GET", queryParameters);

		accessRules = response.getResult();

		return accessRules;
	}

	private String getUserName() {
		IPentahoSession session = PentahoSessionHolder.getSession();
		return session.getName().toString();
	}

	private String getXMLSchema(String schemaUrl) throws SchemaLoadException {

		BufferedReader reader = null;

		try {

			InputStream in = Util.readVirtualFile(schemaUrl);
			reader = new BufferedReader(new InputStreamReader(in));
			StringBuilder out = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				out.append(line);
			}
			reader.close();
			return out.toString();

		} catch (IOException e) {
			throw new SchemaLoadException("Error reading schema file");
		} catch (Exception e) {
			throw new SchemaLoadException("Error loading schema virtual file");
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				throw new SchemaLoadException("Error closing schema file");
			}
		}

	}
	
	private void printDebug(String header, Object value){
		if(debug){
			System.out.println("- CUBEGUARD DEBUG ------------------------------");
			System.out.println(header);
			System.out.println(value);
		}
		
		
	}

}
