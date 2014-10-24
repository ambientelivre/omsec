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

import org.apache.commons.vfs.FileSystemException;
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
	private String stringToBeReplaced, rulesEndpointPath;

	public DynamicMappedRolesSchemaProcessor() throws PropertiesLoadException {

		String propPath = getSolutionRootPath() + "/" + PLUGIN_PATH + "/"
				+ PROPERTIES;

		try {
			setProperties(propPath);
		} catch (IOException e) {
			throw new PropertiesLoadException("Error loading properties file");
		}

		replceString = "true".equals(prop.get("cubeguard.replaceString"));
		stringToBeReplaced = (String) prop.get("cubeguard.replaceString");
		rulesEndpointPath = (String) prop.get("cubeguard.rulesEndpointPath");

	}

	@Override
	public String processSchema(String schemaUrl, Util.PropertyList connectInfo)
			throws DSPException, ParserConfigurationException, ParseException,
			TransformerException {

		String userName = getUserName();
		String schemaText = getXMLSchema(schemaUrl);

		String dataSource = connectInfo.get("DataSource");

		if (replceString)
			schemaText.replace(stringToBeReplaced, userName);

		schemaText = getModifiedSchema(schemaText, dataSource);
		System.out.println(schemaText);
		return schemaText;

	}

	private String getModifiedSchema(String schemaText, String dataSource)
			throws ParserConfigurationException, ParseException,
			TransformerException {

		String rolesText = getXMLRoles(getEndpointResultSet(dataSource));

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

	private String getEndpointResultSet(String dataSource) {

		String accessRules = "";
		Map<String, String> queryParameters = new HashMap<String, String>();
		queryParameters.put("kettleOutput", "Json");
		queryParameters.put("dataSource", dataSource);

		Response response = HttpConnectionHelper.invokeEndpoint("cubeguard",
				rulesEndpointPath, "GET", queryParameters);

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

		} catch (FileSystemException e) {
			throw new SchemaLoadException("Error loading schema virtual file");
		} catch (IOException e) {
			throw new SchemaLoadException("Error reading schema file");
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				throw new SchemaLoadException("Error closing schema file");
			}
		}

	}

}
