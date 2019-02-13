package iudx;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class DbConnector {

	static String index = "";
	static String type = "doc";
	private static RestHighLevelClient client;
	static IndexRequest request;
	static IndexResponse indexResponse;
	static String message;
	static String routingkey;
	static String from;
	private static final String queueName = "DATABASE";
	private static final String broker_hostname = "broker";
	private static final String db_hostname = "elk";
	private static String apikey = "";
	static JSONParser parser;
	static JSONObject _message = null;
	static ConnectionFactory factory;
	static Channel channel;
	static Connection connection;
	static Consumer consumer;
	static com.rabbitmq.client.GetResponse _response;
	static Map<String, Object> jsonMap;
	static boolean json = false;
	static boolean connection_reset = false;
	static String[] _index;
	private final static Logger LOGGER = Logger.getLogger(DbConnector.class.getName());
	
	public static void main(String[] argv) 
	{
		apikey		=	System.getenv("ADMIN_PWD");
		parser		=	new JSONParser();
		_message	=	null;
		factory		=	new ConnectionFactory();
		_index = new String[10];
		
		factory.setUsername("admin");
		factory.setPassword(apikey);
		factory.setVirtualHost("/");
		factory.setHost(broker_hostname);
		factory.setPort(5672);
		
		setConnection();
		setupSubscription();
		consumeData();

		client = new RestHighLevelClient(RestClient.builder(new HttpHost(db_hostname, 9200, "http")));
		request = new IndexRequest(index, type, "");
		jsonMap = new HashMap<>();
	}

	public static void setupSubscription() 
	{
		consumer = new DefaultConsumer(channel) 
		{
			@Override
			public void handleDelivery	(	String consumerTag, Envelope envelope, 
											BasicProperties properties, byte[] body
										)
			throws IOException 
			{
				message		=	new String(body, "UTF-8");
				routingkey	=	envelope.getRoutingKey().toString();
				index 		=	envelope.getExchange().toString();
				from 		=	properties.getUserId();
				
				LOGGER.info("\n" + message + "\n" + routingkey + "\n" + index + "\n" + from);
				System.out.println( routingkey.trim().length() ); 

				if ((from == null) || (from.trim() == "")) 
				{	
					from = "<unverified>";
				}
				
				if (index.contains("/")) 
				{	
					type = index;
					_index = index.split("/");
					index = _index[0];
				} 
				
				else 
				{
					index = "unspecified";
					type = "<unspecified>";
				}	

				if ((routingkey == null) || (routingkey.trim().length() == 0)) 
				{	
					routingkey = "<unspecified>";
				}
				
				try 
				{
					_message = (JSONObject) parser.parse(message);
					json = true;
				}
				
				catch (Exception e) 
				{
					json = false;
				}
				
				posttoElastic();
			}
		};
		
		if(connection_reset) 
		{
			connection_reset = false;
			consumeData();
		}
	}
	
	public static void setConnection() 
	{
		try 
		{
			connection = factory.newConnection();
			channel = connection.createChannel();
		} 
		catch (IOException | TimeoutException e) 
		{
			setConnection();
		}
		
	}
	
	public static void consumeData() 
	{
		
		LOGGER.info(" [*] Database-Connector running. To exit press CTRL+C");
		
		try 
		{
			channel.basicConsume(queueName, true, consumer);
		} 
		catch (IOException e) 
		{
			connection_reset = true;
			setConnection();
			setupSubscription();
		}
	}
	
	public static void posttoElastic() throws IOException 
	{
		LOGGER.info("Writing to DB");
		
		if (json) 
		{
			json = false;
			jsonMap.put("data", _message);
		} 
		
		else 
		{
			jsonMap.put("data", message);
		}
		
		jsonMap.put("topic", routingkey);
		jsonMap.put("from", from);
		jsonMap.put("exchange", type);
		jsonMap.put("postDate", new Date());

		request.index(index);
		request.source(jsonMap);

		LOGGER.info(jsonMap.toString());
		
		indexResponse = client.index(request, RequestOptions.DEFAULT);
		
		LOGGER.info(indexResponse.toString());
	}
}
