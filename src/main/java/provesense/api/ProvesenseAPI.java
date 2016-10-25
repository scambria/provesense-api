package provesense.api;

import static spark.Spark.*;

import java.io.ByteArrayOutputStream;

import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * 
 * @author scambria
 *
 */
public class ProvesenseAPI {

	private static Logger LOGGER = LogManager.getLogger(ProvesenseAPI.class);

	private static String S_E = System.getenv("SPARQL_ENDPOINT");
	private static QueryEngineHTTP httpQuery;

	private static String SPARQL_COUNT_ALL = "SELECT (COUNT(*) as ?count) WHERE {?s ?p ?o}";

	// Excludes RDF and OWL types
	private static String SPARQL_TYPES = 
			"SELECT DISTINCT ?type WHERE { ?s a ?type " + 
			" FILTER(!CONTAINS(STR(?type), 'owl') && !CONTAINS(STR(?type), 'rdf')) }";

	private static String SPARQL_ALL_OF_TYPE = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " + 
			"SELECT * WHERE { ?%s rdfs:subClassOf* %s . ?child a ?%s }";

	private static String SPARQL_COUNT_OF_TYPE = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " + 
			"SELECT (COUNT(*) as ?count) WHERE { ?%s rdfs:subClassOf* %s . ?child a ?%s }";

	private static String SPARQL_ALL_GENERATIONS = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"SELECT ?parent ?child WHERE {  " + 
			" ?child prov:wasInfluencedBy* ?parent " + 
			" FILTER(?child != ?parent) }";

	private static String SPARQL_ALL_GENERATIONS_COUNT = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"SELECT (COUNT(*) AS ?count) WHERE { " + 
			" ?child prov:wasInfluencedBy* ?parent " + 
			" FILTER(?child != ?parent) }";

	private static String SPARQL_ALL_GENERATION_DEPTHS = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"SELECT ?ancestor ?descendant (count(?mid) + 1 as ?depth) WHERE  { " + 
			" ?descendant prov:wasInfluencedBy* ?ancestor . " + 
			" FILTER(?descendant != ?ancestor) " + 
			" OPTIONAL { " + 
			" ?descendant prov:wasInfluencedBy+ ?mid . " + 
			" FILTER(?mid != ?descendant && ?mid != ?ancestor) } } " + 
			"GROUP BY ?ancestor ?descendant " + 
			"ORDER BY DESC(?depth) ?ancestor ?descendant ";

	private static String SPARQL_DESCENDANTS_OF_NODE = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX : <http://provo.ssn.org/provesense#> " + 
			"SELECT DISTINCT ?descendants WHERE  { " + 
			" ?descendants prov:wasInfluencedBy* %s . " + 
			" FILTER(?descendants != %s) }";

	private static String SPARQL_DESCENDANTS_COUNT_OF_NODE = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX : <http://provo.ssn.org/provesense#> " + 
			"SELECT DISTINCT (COUNT(?descendants) as ?count) WHERE  { " + 
			" ?descendants prov:wasInfluencedBy* %s . " + 
			"FILTER(?descendants != %s) }  ";

	private static String SPARQL_DESCENDANTS_OF_NODE_W_DEPTH = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX : <http://provo.ssn.org/provesense#> " + 
			"SELECT ?descendant (count(?mid) + 1 as ?generation_depth) WHERE  { " + 
			" ?descendant prov:wasInfluencedBy* %s . " + 
			" FILTER(?descendant != %s) " + 
			" OPTIONAL{ " + 
			"  ?descendant prov:wasInfluencedBy+ ?mid . " + 
			"  FILTER(?mid != ?descendant && ?mid != %s) } }" + 
			"GROUP BY ?descendant " + 
			"ORDER BY DESC(?generation_depth) ";

	private static String SPARQL_NODE_MAX_GENERATION_DEPTH = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX : <http://provo.ssn.org/provesense#> " + 
			"SELECT (count(?mid) + 1 as ?generation_depth) WHERE  { " + 
			" ?descendant prov:wasInfluencedBy* %s . " + 
			" FILTER(?descendant != %s) " + 
			" OPTIONAL{ " + 
			"  ?descendant prov:wasInfluencedBy+ ?mid . " + 
			"  FILTER(?mid != ?descendant && ?mid != %s) } } " + 
			"GROUP BY ?descendant " + 
			"ORDER BY DESC(?generation_depth) " + 
			"LIMIT 1 ";

	private static String SPARQL_ANCESTORS_OF_NODE = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX : <http://provo.ssn.org/provesense#> " +
			"SELECT DISTINCT ?ancestors WHERE  { " + 
			"%s prov:wasInfluencedBy* ?ancestors . " + 
			"FILTER(%s != ?ancestors) }  ";

	private static String SPARQL_ANCESTORS_COUNT_OF_NODE = 
			"PREFIX prov: <http://www.w3.org/ns/prov#> " + 
			"PREFIX : <http://provo.ssn.org/provesense#> " + 			
			"SELECT DISTINCT (COUNT(?ancestors) as ?count) WHERE  {   " +  
			"%s prov:wasInfluencedBy* ?ancestors . " + 
			"FILTER(%s != ?ancestors) }  ";

	/**
	 * 
	 * @param sparql
	 * @return String Query response or an error
	 */
	private static String query(String sparql) {
		try {
			LOGGER.debug(sparql);
			Query query = QueryFactory.create(sparql, Syntax.syntaxARQ);
			httpQuery = new QueryEngineHTTP(S_E, query);
			ResultSet results = httpQuery.execSelect();
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(outputStream, results);
			String output = new String(outputStream.toByteArray());
			LOGGER.debug(output);
			return output;
		} catch (Exception e) {
			LOGGER.error(e);
			return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";
		}
	}

	/**
	 * 
	 * @param sparql
	 * @return boolean True if ASK query passes
	 */
	private static boolean ask(String sparql) {
		try {
			LOGGER.debug(sparql);
			Query query = QueryFactory.create(sparql, Syntax.syntaxARQ);
			httpQuery = new QueryEngineHTTP(S_E, query);
			boolean result = httpQuery.execAsk();
			LOGGER.debug(result);
			return result;
		} catch (Exception e) {
			LOGGER.error(e);
			return false;
		}
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LOGGER.info("Starting ProvesenseAPI, using SPARQL ENDPOINT: " + S_E);

		get("/provesense", (request, response) -> {
			try { 
				LOGGER.debug("/provesense request");
				return ProvesenseAPI.query(SPARQL_COUNT_ALL);				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}
		});

		// Retrieve all types in graph that have instance data
		// example: prov:Agent, prov:Person, ssn:Observation
		// note - excludes OWL and RDF namespaces
		get("/provesense/types", (request, response) -> {
			try {
				LOGGER.debug("/provesense/types request");
				return ProvesenseAPI.query(SPARQL_TYPES);				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}			
		});

		// Retrieve all instances of a given type
		// example: prov:Agent, prov:Person, ssn:Observation
		get("/provesense/type/instances/:type", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/type/instances/:type request");
				String type = request.params(":type");
				LOGGER.debug("type: " + type);
				// TODO: Validate against schema
				// ProvesenseAPI.query(SPARQL_TYPES);
				if (type != null) {
					String varType = type.toLowerCase().split(":")[1];
					return ProvesenseAPI.query(String.format(SPARQL_ALL_OF_TYPE, varType, type, varType));
				}
				return "{'Status': 'invalid type: " + type + "'}";				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}			
		});

		// Retrieve instance count of a given type
		// example: prov:Agent, prov:Person, ssn:Observation
		get("/provesense/type/count/:type", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/type/count/:type request");
				String type = request.params(":type");
				LOGGER.debug("type: " + type);
				// TODO: Validate against schema
				// ProvesenseAPI.query(SPARQL_TYPES);
				if (type != null) {
					String varType = type.toLowerCase().split(":")[1];
					return ProvesenseAPI.query(String.format(SPARQL_COUNT_OF_TYPE, varType, type, varType));
				}
				return "{'Status': 'invalid type: " + type + "'}";				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}			
		});
		
		// Retrieve all generations in graph
		get("/provesense/generations", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/generations request");
				return ProvesenseAPI.query(SPARQL_ALL_GENERATIONS);
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}			
		});

		// Retrieve all generations count in graph
		get("/provesense/generations/count", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/generations/count request");
				return ProvesenseAPI.query(SPARQL_ALL_GENERATIONS_COUNT);				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}			
		});

		// Retrieve generation depths for all nodes in graph
		get("/provesense/generations/depth", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/generations/depth request");
				return ProvesenseAPI.query(SPARQL_ALL_GENERATION_DEPTHS);				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}
		});

		// Retrieve the depth of a nodes max generation in graph
		get("/provesense/generations/depth/max/:id", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/generations/depth/:id request");
				String id = request.params(":id");
				LOGGER.debug("id: " + id);
				// TODO: validate id
				return ProvesenseAPI.query(String.format(SPARQL_NODE_MAX_GENERATION_DEPTH, id, id, id));				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}			
		});

		// Retrieve the depth of a nodes generation in graph
		get("/provesense/generations/depth/:id", (request, response) -> {
			try {
				LOGGER.debug("/provesense/generations/depth/:id request");
				String id = request.params(":id");
				LOGGER.debug("id: " + id);
				// TODO: validate id
				return ProvesenseAPI.query(String.format(SPARQL_DESCENDANTS_OF_NODE_W_DEPTH, id, id, id));				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}		
		});

		// Retrieve all descendants of node w/ given URI
		get("/provesense/descendants/:id", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/descendants/:id request");
				String id = request.params(":id");
				LOGGER.debug("id: " + id);
				// TODO: validate id
				return ProvesenseAPI.query(String.format(SPARQL_DESCENDANTS_OF_NODE, id, id));				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}					
		});

		// Retrieve descendant count of node w/ given URI
		get("/provesense/descendants/count/:id", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/descendants/count/:id request");
				String id = request.params(":id");
				LOGGER.debug("id: " + id);
				// TODO: validate id
				return ProvesenseAPI.query(String.format(SPARQL_DESCENDANTS_COUNT_OF_NODE, id));				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}					
		});

		// Retrieve all ancestors of node w/ given id
		get("/provesense/ancestors/:id", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/ancestors/:id request");
				String id = request.params(":id");
				LOGGER.debug("id: " + id);
				return ProvesenseAPI.query(String.format(SPARQL_ANCESTORS_OF_NODE, id));				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}					
		});

		// Retrieve # of ancestors of node w/ given URI, aka (ie) 4th generation
		get("/provesense/ancestors/count/:id", (request, response) -> {
			try { 
				LOGGER.debug("/provesense/ancestors/:id request");
				String id = request.params(":id");
				LOGGER.debug("id: " + id);
				return ProvesenseAPI.query(String.format(SPARQL_ANCESTORS_COUNT_OF_NODE, id));				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}								
		});

		get("/provesense/custom/:sparql", (request, response) -> {
			try {
				LOGGER.debug("/provesense/custom/:sparql request");
				String sparql = request.params(":sparql");
				LOGGER.info("custom/:sparql: " + sparql);
				String result = ProvesenseAPI.query(sparql);
				LOGGER.info(result);
				return result;				
			}
			catch (Exception e) {
				LOGGER.error(e);
				return "{'status': 'FAIL', 'error':" + e.getMessage() + "}";				
			}								
		});

		// May want to use splats for graph retrieval
		// matches "GET /say/hello/to/world"
		// request.splat()[0] is 'hello' and request.splat()[1] 'world'
		// get("/say/*/to/*", (request, response) -> {
		// return "Number of splat parameters: " + request.splat().length;
		// });
		// Retrieve the graph (DESCRIBE) of node w/ given id
		get("/provesense/graph/id/:id", (request, response) -> {
			throw new NotImplementedException();
		});
		// Retrieve the graph (DESCRIBE) of nodes w/ given type
		get("/provesense/graph/type/:type", (request, response) -> {
			throw new NotImplementedException();
		});
		// Retrieve the graph (DESCRIBE) of nodes w/ given type
		get("/provesense/graph/location/:location", (request, response) -> {
			throw new NotImplementedException();
		});
	}
}
// curl http://localhost:4567/provesense
