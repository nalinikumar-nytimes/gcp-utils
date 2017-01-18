package nyt.utils;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class Publisher{

	private final Pubsub pubsub;
	private final String topicArn;

	public static final String PUBSUB_TIMESTAMP_LABEL_KEY = "timestamp_ms";

	public Publisher(Pubsub pubsub, String topicArn){
		this.pubsub = pubsub;
		this.topicArn =topicArn;
	}

	public  void dispatch(String message, long time) throws Exception{
		PubsubMessage pubsubMessage = new PubsubMessage();
		pubsubMessage.encodeData(message.getBytes("UTF-8"));
		List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
		PublishRequest publishRequest =
				new PublishRequest().setMessages(messages);
		pubsubMessage.setAttributes(
				ImmutableMap.of(PUBSUB_TIMESTAMP_LABEL_KEY, Long.toString(time)));
		PublishResponse publishResponse = pubsub.projects().topics()
				.publish(topicArn, publishRequest)
				.execute();
		publishResponse.getMessageIds();
	}

	public void dispatch(List<String> bulk)  throws Exception {
		if(bulk.isEmpty()) return;
		List<PubsubMessage> messages = new ArrayList<PubsubMessage>();
		for(String s : bulk){
			PubsubMessage pubsubMessage = new PubsubMessage();
			pubsubMessage.encodeData(s.getBytes("UTF-8"));
			messages.add(pubsubMessage);
		}
		
		PublishRequest publishRequest = new PublishRequest().setMessages(messages);
		pubsub.projects().topics().publish(topicArn, publishRequest).execute();
	}

	
}

