package org.keedio.storm.bolt.filter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.keedio.storm.bolt.filter.metrics.MetricsController;
import org.keedio.storm.bolt.filter.metrics.MetricsEvent;
import org.keedio.storm.bolt.filter.metrics.SimpleMetric;
import org.apache.log4j.*;

import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static backtype.storm.utils.Utils.tuple;

public class SerializeBolt extends BaseRichBolt {

	public static final Logger LOG = Logger
			.getLogger(SerializeBolt.class);
	private static final Pattern hostnamePattern =
		    Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");

	String allowMessages, denyMessages;
    private Date lastExecution = new Date();
    private String groupSeparator;
    private Map<String, Pattern> allPatterns;
    private OutputCollector collector;
    private String gangliaServer;
    private int gangliaPort;
    private int refreshTime;
	private MetricsController mc;
 
	
    public MetricsController getMc() {
		return mc;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		//Inicializmos las metricas
		mc = new MetricsController();
		
	}


	@Override
	public void execute(Tuple input) {
		
		LOG.debug("Serialize message: execute");

		// Añadimos al throughput e inicializamos el date
        Date actualDate = new Date();
        long aux = (actualDate.getTime() - lastExecution.getTime())/1000;
        lastExecution = actualDate;
        
        // Registramos para calculo de throughput
        mc.manage(new MetricsEvent(MetricsEvent.UPDATE_THROUGHPUT, aux));
		
		String message = input.getStringByField("message");
		JSONObject json = (JSONObject)input.getValueByField("extraData");
		
		JSONObject obj = new JSONObject();
		obj.put("message", message);
		obj.put("extraData", json);
			
		collector.emit(input, new Values(obj.toString()));
		collector.ack(input);
			
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}
