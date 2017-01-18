package nyt.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.Empty;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;

public class PubSubUtils {

	private static Pubsub pubsub = null;

	private static Log m_log = LogFactory.getLog(PubSubUtils.class);

	private static GenericObjectPool<Pubsub> pool;

	private static PullRequest pullRequest;

	private static final int BATCH_SIZE = 100;

	private static final String SUBSCRIBER_NAME = "projects/gcp-data-poc-1208/subscriptions/spark";

	public static Pubsub getPubsubClient() throws IOException {
		if (null == pubsub) {
			pubsub = createPubsubClient(Utils.getDefaultTransport(),
					Utils.getDefaultJsonFactory());
		}
		return pubsub;
	}

	public static PullRequest getPullRequest() {
		if (null == pullRequest) {
			pullRequest = new PullRequest().setReturnImmediately(false)
					.setMaxMessages(BATCH_SIZE);
		}
		return pullRequest;
	}

	public static void publishNcopies(String msg, String topicArn, int numCopies)
			throws UnsupportedEncodingException, IOException {
		List<String> lines = Collections.nCopies(numCopies, msg);

		if (getPubsubClient() != null) {
			if (lines.isEmpty())
				return;

			List<PubsubMessage> messages = new ArrayList<PubsubMessage>();
			for (String s : lines) {
				PubsubMessage pubsubMessage = new PubsubMessage();
				pubsubMessage.encodeData(s.getBytes("UTF-8"));
				messages.add(pubsubMessage);
			}

			PublishRequest publishRequest = new PublishRequest()
					.setMessages(messages);
			pubsub.projects().topics().publish(topicArn, publishRequest)
					.execute();
		}
	}
	
	public static void publishNcopiesAsJson(String msg, String topicArn, int numCopies)
			throws UnsupportedEncodingException, IOException {
		List<String> lines = Collections.nCopies(numCopies, msg);

		if (getPubsubClient() != null) {
			if (lines.isEmpty())
				return;
			
			int i=0;

			List<PubsubMessage> messages = new ArrayList<PubsubMessage>();
			for (String s : lines) {
				PubsubMessage pubsubMessage = new PubsubMessage();
				pubsubMessage.encodeData(s.getBytes("UTF-8"));
				messages.add(pubsubMessage);
			}

			PublishRequest publishRequest = new PublishRequest()
					.setMessages(messages);
			pubsub.projects().topics().publish(topicArn, publishRequest)
					.execute();
		}
	}

	public static void publishSingle(String msg, String topicArn)
			throws Exception {
		List<PubsubMessage> messages = new ArrayList<PubsubMessage>();
		PubsubMessage pubsubMessage = new PubsubMessage();
		//pubsubMessage.setPublishTime(System.currentTimeMillis() + "");
		//pubsubMessage.set("ts", System.currentTimeMillis());
		pubsubMessage.encodeData(msg.getBytes("UTF-8"));
		messages.add(pubsubMessage);

		PublishRequest publishRequest = new PublishRequest()
				.setMessages(messages);
		GenericObjectPool<Pubsub> clientPool = getClientPool();
		Pubsub borrowObject = getClientPool().borrowObject();
		borrowObject.projects().topics().publish(topicArn, publishRequest)
				.execute();
		clientPool.returnObject(borrowObject);
	}

	
	public static Empty acknowledge(List<String> ackIds) throws Exception {
		AcknowledgeRequest ackRequest = new AcknowledgeRequest()
				.setAckIds(ackIds);
		return pubsub.projects().subscriptions()
				.acknowledge(SUBSCRIBER_NAME, ackRequest).execute();
	}

	// A factory method that allows you to use your own HttpTransport
	// and JsonFactory.
	static Pubsub createPubsubClient(HttpTransport httpTransport,
			JsonFactory jsonFactory) throws IOException {
		Preconditions.checkNotNull(httpTransport);
		Preconditions.checkNotNull(jsonFactory);
		GoogleCredential credential = GoogleCredential.getApplicationDefault(
				httpTransport, jsonFactory);
		// In some cases, you need to add the scope explicitly.
		if (credential.createScopedRequired()) {
			credential = credential.createScoped(PubsubScopes.all());
		}
		// Please use custom HttpRequestInitializer for automatic
		// retry upon failures. We provide a simple reference
		// implementation in the "Retry Handling" section.
		HttpRequestInitializer initializer = new RetryHttpInitializerWrapper(
				credential);
		return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
				.build();
	}
	

	public static GenericObjectPool<Pubsub> getClientPool() {
		synchronized (m_log) {
			if (pool == null) {
				GenericObjectPoolConfig config = new GenericObjectPoolConfig();
				config.setMaxTotal(30);
				PubSubClientFactory factory = new PubSubClientFactory();
				pool = new GenericObjectPool(factory, config);
			}
		}

		return pool;

	}
	
	public static void main( String[] args )
    {
	  //java.util.Date date= new java.util.Date();
	  //System.out.println(date);
	 
		try {
			int i=0;
			while(i++ < 10)
			 PubSubUtils.publishNcopies("{\"message\":\"test\",\"messageId\":\"test\"}", "projects/gcp-data-poc-1208/topics/spark", 1000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	/*	SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		try {
			Date date = fmt.parse("2016-03-08T22:50:02.577Z");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			System.out.println(calendar.getTimeInMillis());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    */}
	
	

}