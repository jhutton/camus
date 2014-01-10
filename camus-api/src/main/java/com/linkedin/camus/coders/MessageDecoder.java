package com.linkedin.camus.coders;

import java.util.Properties;

/**
 * Decoder interface. Implementations should return a CamusWrapper with timestamp
 * set at the very least.  Camus will instantiate a descendant of this class
 * based on the property ccamus.message.decoder.class.
 * @author kgoodhop
 *
 * @param <R> The type of the decoded message
 */
public abstract class MessageDecoder<R> {
	protected Properties props;
	protected String topicName;

	public void init(Properties props, String topicName){
	    this.props = props;
        this.topicName = topicName;
	}

	public abstract CamusWrapper<R> decode(byte[] message) ;

}
