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

	/**
	 * Decode the message contained in the specified message bytes and set the
	 * value in the specified wrapper.
	 *
	 * @param message the message
	 * @param wrapper the wrapper
	 * @return true if a value was successfully decoded and set, else false
	 */
	public abstract boolean decode(byte[] message, CamusWrapper<R> wrapper) ;

}
