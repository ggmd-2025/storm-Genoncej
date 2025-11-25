package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
// import org.json.JSONObject;
// import org.json.JSONArray;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;



/**
 * Sample of stateless operator
 * @author lumineau
 *
 */
public class MyTortoiseBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
	private OutputCollector collector;
	
	
	public MyTortoiseBolt () {
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {
	
		try {
			String json = t.getValueByField("json").toString();
			// JSONObject jsonObj = new JSONObject(json);
			// JSONArray runners = jsonObj.getJSONArray("runners");
			// JSONObject first = runners.getJSONObject(0);

			// int id = first.getInt("id");
			// int top = first.getInt("top");
			// String nom = "test";
			// int total = first.getInt("total");
			// int maxcel = first.getInt("maxcel");
			// int nbCellsParcourus = total - top;

			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readTree(json);
			JsonNode runners =  jsonNode.get("runners");
			JsonNode first = runners.get(0);
			int id =  first.get("id").asInt();
			int top =  first.get("top").asInt();
			String nom = "test";
			int total =  first.get("total").asInt();
			int maxcel =  first.get("maxcel").asInt();
			int nbCellsParcourus = total - top;

			logger.info( "=> " + json + " treated!");
			collector.emit(new Values(id, top, nom, nbCellsParcourus, total, maxcel));
			collector.ack(t);
		}catch (Exception e){
			System.err.println("Empty tuple.");
		}
		return;
		
	}
	
	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
	}
		

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		
	}
	
	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}