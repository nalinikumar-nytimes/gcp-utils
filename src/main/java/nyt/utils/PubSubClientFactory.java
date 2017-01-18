package nyt.utils;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.pubsub.Pubsub;

public class PubSubClientFactory  implements PooledObjectFactory<Pubsub> {

	public void activateObject(PooledObject<Pubsub> arg0) throws Exception {
		// TODO Auto-generated method stub
	}

	public void destroyObject(PooledObject<Pubsub> obj) throws Exception {
		 //obj.getObject().
		
	}

	public PooledObject<Pubsub> makeObject() throws Exception {
		// TODO Auto-generated method stub
		return new DefaultPooledObject<Pubsub>(PubSubUtils.createPubsubClient(
				Utils.getDefaultTransport(), Utils.getDefaultJsonFactory()));
	}

	public void passivateObject(PooledObject<Pubsub> arg0) throws Exception {
		// TODO Auto-generated method stub
		
	}

	public boolean validateObject(PooledObject<Pubsub> arg0) {
		// TODO Auto-generated method stub
		return true;
	}

}