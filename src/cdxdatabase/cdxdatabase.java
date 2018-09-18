package cdxdatabase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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

public class cdxdatabase {

	static String index = "";
	private static RestHighLevelClient client;
	static IndexRequest request;
	static IndexResponse indexResponse;
	static String message;
	static String routingkey;
	static String from;
	private static final String queueName = "database";
	private static final String hostname = "broker";
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

	public static void main(String[] argv) throws Exception {

		readdbpassword();
		parser = new JSONParser();
		_message = null;
		factory = new ConnectionFactory();
		factory.setUsername(queueName);
		factory.setPassword(apikey);
		factory.setVirtualHost("/");
		factory.setHost(hostname);
		factory.setPort(5672);

		connection = factory.newConnection();
		channel = connection.createChannel();
		client = new RestHighLevelClient(RestClient.builder(new HttpHost("elasticsearch", 9200, "http")));
		request = new IndexRequest(index, "doc", "");
		jsonMap = new HashMap<>();

		System.out.println(" [*] Database-Connector running. To exit press CTRL+C");

		while (true) {

			_response = channel.basicGet(queueName, true);

			if (_response != null) {

				message = new String(_response.getBody());
				routingkey = _response.getEnvelope().getRoutingKey().toString();
				index = _response.getEnvelope().getExchange().toString();

				// Get the User ID from properties, if not found set to unknown

				from = "<unverified>";

				if (message == null) {
					continue;
				} else {
					try {
						_message = (JSONObject) parser.parse(message);
						json = true;

					} catch (Exception e) {
						json = false;
					}
					posttoElastic();
				}
			} else {
				Thread.sleep(5000);
				continue;
			}
		}
	}

	public static void posttoElastic() throws IOException {

		if (json) {
			jsonMap.put("data", _message);
		} else {
			jsonMap.put("data", message);
		}
		jsonMap.put("topic", routingkey);
		jsonMap.put("from", from); // from must be changed to user name from properties while publishing to achieve
									// this. https://www.rabbitmq.com/validated-user-id.html
		jsonMap.put("insertDate", new Date());

		request.index(index);
		request.source(jsonMap);

		indexResponse = client.index(request, RequestOptions.DEFAULT);
	}

	public static void readdbpassword() {
		try {
			BufferedReader br = new BufferedReader(new FileReader("/etc/databasepwd"));
			apikey = br.readLine();
			br.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}