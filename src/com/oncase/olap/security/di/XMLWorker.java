package com.oncase.olap.security.di;

import java.io.ByteArrayOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.keyvalue.MultiKey;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XMLWorker {

	public static final int DENIED_HIERARCHIES = 0;
	public static final int ALLOWED_HIERARCHIES = 1;
	public static final int DENIED_CUBES = 2;
	public static final int SCHEMA_GRANT_ALL = 3;
	

	public static final int I_USER = 0;
	public static final int I_SCHEMA = 1;
	public static final int I_CUBE = 2;
	public static final int I_HIERARCHY = 3;
	public static final int I_MEMBER = 4;
	public static final int I_TYPE = 5;
	
	public static final Class[] TYPES = {
		String.class,String.class,String.class,String.class,String.class,Integer.class,
	};

	private ArrayList<Object[]> resultset;
	private DocumentBuilder documentBuilder;
	private DocumentBuilderFactory documentFactory;
	private Document doc;

	private Map<String, String> cubeGrants;
	private Map<String, Element> cubeGrantsElm;
	private boolean schemaGrantAll; 

	// key: cube,hierarachy
	private Map<MultiKey, String> hierarachyGrants;
	private Map<MultiKey, Element> hierarachyGrantsElm;

	private Map<MultiKey, String> memberGrants;

	Element schemaGrant;

	/**
	 * @param resultset
	 *            The mixed resultset from: - Allowed Hierarchies and Measures
	 *            (1) - Denied Hierarchies and Measures (2) - Denied cubes (3)
	 *            All put together
	 */
	public XMLWorker(ArrayList<Object[]> resultset)
			throws ParserConfigurationException {
		
		this.resultset = resultset;
		documentFactory = DocumentBuilderFactory.newInstance();
		documentBuilder = documentFactory.newDocumentBuilder();
		initDocument();
		createCollections();
		if(!schemaGrantAll){
			appendCubeGrants();
			appendHierarchyGrants();
			appendMemberGrants();
			verifySchemaGrantHasChildren();
		}
		
	}
	
	private void verifySchemaGrantHasChildren(){
		if(!schemaGrant.hasChildNodes()){
			schemaGrant.setAttribute("access", "none");
		}
	}

	/**
	 * Starts the document from scratch.
	 */
	private void initDocument() {
		
		doc = documentBuilder.newDocument();
		Element role = doc.createElement("Role");
		role.setAttribute("name", "Authenticated");
		doc.appendChild(role);
		schemaGrant = doc.createElement("SchemaGrant");
		schemaGrant.setAttribute("access", "all");
		role.appendChild(schemaGrant);
		
	}

	/**
	 * Creates collections for the Access control entities
	 */
	private void createCollections() {

		cubeGrants = new HashMap<String, String>();
		hierarachyGrants = new HashMap<MultiKey, String>();
		memberGrants = new HashMap<MultiKey, String>();
		
		Set<String> deniedCubes = new HashSet<String>();
		Iterator<Object[]> it = resultset.iterator();

		while (it.hasNext()) {
			Object[] next = it.next();
			String cubeName = (String) next[I_CUBE];
			
			//schemaGrantAllCount
			if((int) next[I_TYPE] == SCHEMA_GRANT_ALL)
				schemaGrantAll=true;

			// cubeGrants
			boolean isCubeDenied = (int) next[I_TYPE] == DENIED_CUBES;
			cubeGrants.put(cubeName, isCubeDenied ? "none" : "all");
			
			if (isCubeDenied)
				deniedCubes.add(cubeName);

			if ((int) next[I_TYPE] == ALLOWED_HIERARCHIES || 
					(int) next[I_TYPE] == DENIED_HIERARCHIES) {

				// hierarchyGrants
				final String member = (String) next[I_MEMBER];
				final boolean allHierarchy = "all".equals(member);
				final boolean allowType = (int) next[I_TYPE] == ALLOWED_HIERARCHIES;
				final String hierarchyAccess = allowType && allHierarchy ? "all"
						: !allowType && allHierarchy ? "none" : "custom";
				final String hierarachyName = (String) next[I_HIERARCHY];
				hierarachyGrants.put(new MultiKey(cubeName, hierarachyName),
						hierarchyAccess);

				// memberGrants
				if (!allHierarchy) {
					String memberAccess = allowType ? "all" : "none";
					memberGrants.put(new MultiKey(cubeName, hierarachyName,
							member), memberAccess);
				}

			}
		}

		// Making sure, in case of double reference.
		Iterator<String> itDen = deniedCubes.iterator();
		while (itDen.hasNext()) {
			cubeGrants.put(itDen.next(), "none");
		}

	}

	/**
	 * Appends all the cubeGrants to the document
	 */
	private void appendCubeGrants() {

		cubeGrantsElm = new HashMap<String, Element>();
		Iterator<String> it = cubeGrants.keySet().iterator();

		while (it.hasNext()) {

			String cube = it.next();
			String access = cubeGrants.get(cube);
			Element cubeGrant = doc.createElement("CubeGrant");
			cubeGrant.setAttribute("cube", cube);
			cubeGrant.setAttribute("access", access);
			cubeGrantsElm.put(cube, cubeGrant);
			schemaGrant.appendChild(cubeGrant);

		}

	}

	/**
	 * Appends all the schemaGrants to the document
	 */
	private void appendHierarchyGrants() {

		Iterator<MultiKey> it = hierarachyGrants.keySet().iterator();
		hierarachyGrantsElm = new HashMap<MultiKey, Element>();

		while (it.hasNext()) {

			MultiKey key = it.next();
			String cube = (String) key.getKey(0);
			String hierarchy = (String) key.getKey(1);
			String access = hierarachyGrants.get(key);
			Element cubeGrant = cubeGrantsElm.get(cube);

			if (cubeGrant != null) {

				Element hierarchyGrant = doc.createElement("HierarchyGrant");
				hierarchyGrant.setAttribute("hierarchy", hierarchy);
				hierarchyGrant.setAttribute("access", access);

				//Partial rollupPolicity
				hierarchyGrant.setAttribute("rollupPolicy", "partial");


				cubeGrant.appendChild(hierarchyGrant);
				hierarachyGrantsElm.put(key, hierarchyGrant);

			}

		}

	}

	/**
	 * Appends all the memberGrants to the document
	 */
	private void appendMemberGrants() {

		Iterator<MultiKey> it = memberGrants.keySet().iterator();

		while (it.hasNext()) {

			MultiKey key = it.next();
			String cube = (String) key.getKey(0);
			String hierarchy = (String) key.getKey(1);
			String member = (String) key.getKey(2);
			String access = memberGrants.get(key);

			Element hierarachyGrant = hierarachyGrantsElm.get(new MultiKey(
					cube, hierarchy));

			if (hierarachyGrant != null) {

				Element memberGrant = doc.createElement("MemberGrant");
				memberGrant.setAttribute("member", member);
				memberGrant.setAttribute("access", access);
				hierarachyGrant.appendChild(memberGrant);

				// Fixes a possible HierarchyGrant[all/none] with children
				if (!hierarachyGrant.getAttribute("access").equals("custom"))
					hierarachyGrant.setAttribute("access", "custom");

			}

		}

	}

	/**
	 * @return the XML Document as String.
	 */
	public String getXML() throws TransformerException {
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		DOMSource domSource = new DOMSource(doc);
		StreamResult result = new StreamResult(bs);

		TransformerFactory transformerFactory = TransformerFactory
				.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty("omit-xml-declaration", "yes");
		transformer.transform(domSource, result);

		return bs.toString();
	}

}