package org.keedio.storm.bolt.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;
import org.keedio.storm.spout.simple.SimpleSpout;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;

public class TopologyTest {

	@Test
	public void testBasicTopology() {
		

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {

				String ret = "{\"message\":\"fsfsdf\",\"extraData\":{\"nombre\":\"Rodrigo\",\"Apellidos\":\"Olmo\"}}";
				
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new SimpleSpout(), 3);
				builder.setBolt("2", new DeserializeBolt(), 4).shuffleGrouping("1");
				builder.setBolt("3", new SerializeBolt(), 4).shuffleGrouping("2");

				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values(ret.getBytes()));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);

				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				List tupleValues = (ArrayList)Testing.readTuples(result, "3").get(0);

				Assert.assertTrue("Solo existe un elemento en la tupla", tupleValues.size() == 1);
				JSONParser parser = new JSONParser();  
				
				// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
				JSONObject obj;
				obj = (JSONObject)tupleValues.get(0);
				Assert.assertNotNull("Existe el elemento message", obj.get("message"));
			}

		});
	}
	
	
}
