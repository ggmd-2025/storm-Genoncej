package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit2Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;

/**
 * 
 * @author lumineau
 * Topologie test permettant d'écouter le Master Input 
 *
 */
public class TopologyT2 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = Integer.parseInt(args[0]);
		int portOUTPUT = Integer.parseInt(args[1]);
    	
    	InputStreamSpout spout = new InputStreamSpout("client", portINPUT);
    	TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("masterStream", spout);
        builder.setBolt("mytortoisebolt", new MyTortoiseBolt(), nbExecutors).shuffleGrouping("masterStream");
        builder.setBolt("exit", new Exit2Bolt(portOUTPUT), nbExecutors).shuffleGrouping("mytortoisebolt");
       
        Config config = new Config();
        StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
	}
		
	
}