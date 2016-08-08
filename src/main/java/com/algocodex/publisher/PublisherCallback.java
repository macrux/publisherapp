package com.algocodex.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wombat.mama.MamaPublisher;
import com.wombat.mama.MamaPublisherCallback;


public class PublisherCallback implements MamaPublisherCallback {

	private static final Logger logger = LoggerFactory.getLogger(PublisherCallback.class.getSimpleName());

	//There is a problem and next line throws a NullPointerException when pub.getSymbol() is called
	public void onCreate(MamaPublisher pub) {
		// pub.getSymbol()
		logger.info("onPublishCreate: " + pub.getClosure().toString());
	}

	public void onDestroy(MamaPublisher pub) {
		// pub.getSymbol()
		logger.info("onPublishDestroy: " + pub.getClosure().toString());
	}

	public void onError(MamaPublisher pub, short status, String info) {
		// pub.getSymbol()
		logger.error("onPublishError: " + pub.getClosure().toString() + " " + status + " " + info);
	}

}
