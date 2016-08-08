package com.algocodex.publisher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wombat.common.WombatException;
import com.wombat.mama.Mama;
import com.wombat.mama.MamaBridge;
import com.wombat.mama.MamaDateTime;
import com.wombat.mama.MamaMsg;
import com.wombat.mama.MamaMsgStatus;
import com.wombat.mama.MamaPublisher;
import com.wombat.mama.MamaQueue;
import com.wombat.mama.MamaQueueGroup;
import com.wombat.mama.MamaReservedFields;
import com.wombat.mama.MamaTransport;
import com.wombat.mama.MamaTransportTopicListener;

public class MamaTopicPublisher {

	private MamaBridge bridge = null;
	private MamaQueue queue = null;
	// private MamaMsg myMsg = null;
	private MamaTransport transport = null;
	private MamaPublisher publisher = null;
	private MamaQueueGroup queueGroup = null;	
	private MamaTransportTopicListener transportTopicListener = null;
	
	private long msgCounter = 1;
	private boolean pubCallback = false;
	
	private MamaDateTime sendTime;
	private MamaStartRunnable publisherRunnable = null;
	private ExecutorService publisherExecutor;

	public  final String name;
	private final String middleware;
	private final String tport;
	private final String topic;
	
	private static Logger logger = LoggerFactory.getLogger(MamaTopicPublisher.class.getSimpleName());

	public MamaTopicPublisher(String name, String tport, String middleware, String topic) {
		this.name = name;
		this.middleware = middleware;
		this.tport = tport;
		this.topic = topic;
	}

	/**
	 * Initialize Mama and create a basic transport.
	 */
	private void initializeMama() {

		try {
			bridge = Mama.loadBridge(middleware);
			Mama.open();
			queue = Mama.getDefaultQueue(bridge);
			queueGroup = new MamaQueueGroup(1, bridge);
			// myMsg = new MamaMsg();
			
			/* Add transport and transport topic listeners */
			transport = new MamaTransport();
			transportTopicListener = new MamaTransportTopicListener();
			transport.addTransportTopicListener(transportTopicListener);
			transport.create(tport, bridge);
		} catch (WombatException e) {
			e.printStackTrace();
			logger.error("Exception initializing mama", e);
			System.exit(1);
		}
	}

	private void createPublisher() {
		try {
			publisher = new MamaPublisher();
			if (pubCallback) {
				PublisherCallback callback = new PublisherCallback();
				// here we send topicName as a closure object because there is a
				// problem with the publisher callback, it cannot retrieve the symbol()
				publisher.create(transport, queueGroup.getNextQueue(), topic, null, callback, topic);
			} else {
				publisher.create(transport, topic);
			}

		} catch (WombatException e) {
			e.printStackTrace();
			logger.error("Exception creating publisher: ", e);
			System.exit(1);
		}
	}
	
	public long publishMessage(MamaMsg myMsg) {
		try {
			// TODO revisar si es mas conveniente tener un MamaMsg listo para ser enviado
			myMsg.addI32(MamaReservedFields.MsgStatus.getName(), MamaReservedFields.MsgStatus.getId(), MamaMsgStatus.STATUS_OK);
			myMsg.addI32(MamaReservedFields.SeqNum.getName(), MamaReservedFields.SeqNum.getId(), (int) msgCounter);
			myMsg.addString(MamaReservedFields.FeedHost.getName(), MamaReservedFields.FeedHost.getId(), topic);
			sendTime.setToNow();
			myMsg.addString(MamaReservedFields.SendTime.getName(), MamaReservedFields.SendTime.getId(), sendTime.getAsString());		

			logger.debug("Publishing message {} to {} ", msgCounter, topic);
			publisher.send(myMsg);
			msgCounter++;

		} catch (WombatException e) {
			e.printStackTrace();
			logger.error("Exception publishing message ", e);
			System.exit(1);
		}

		return msgCounter;
	}

	/**
	 * Set the publisher configuration.
	 * 
	 * @param logLevel.
	 *            Level for OpenMAMA loggin level. It can only be Level.FINE,
	 *            Level.FINER or Level.FINEST
	 * @param enablePubCallback.
	 *            Boolean specifiying weather listen for publisher callbacks or
	 *            not.
	 */
	public void setOptions(Level logLevel, boolean enablePubCallback) {
		Mama.enableLogging(logLevel);
		pubCallback = enablePubCallback;
	}

	public void start() {
		
		logger.info("Starting {} with:\n" 
				+ "  topic:\t{}\n" 
				+ "  transport:\t{}\n" 
				+ "  middleware:\t{}\n",
				this.name,
				this.topic, 
				this.tport, 
				this.middleware);
		
		Thread.currentThread().setName(name);
		initializeMama();
		createPublisher();

		publisherRunnable = new MamaStartRunnable(bridge);
		Thread pusblisherThread = new Thread(publisherRunnable);
		pusblisherThread.setName(name+"-start");
		publisherExecutor = Executors.newSingleThreadExecutor();		
		publisherExecutor.execute(pusblisherThread);
		/* MamaDateTime() must be initialized after start openmama*/
		sendTime = new MamaDateTime();
	}	

	public void stop() {
		Thread stop = new Thread(new Thread() {
			public void run() {
				shutdown();
			}
		});
		stop.setName(name+"-stop");
		stop.start();
	}
	
	private void shutdown() {
		logger.info("Stopping ...");
		try {
			logger.debug("executing: publisher.destroy() ...");
			publisher.destroy();
			// TODO check if this is necessary and check the step guide to close and openmama app
			// Thread.sleep(0); 
			// let queued events finish
			logger.debug("executing: Mama.stop(bridge) ...");
			Mama.stop(bridge);

			do {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
			} while (!publisherRunnable.hasCompleted());

			logger.debug("executing: transport.destroy() ...");
			transport.destroy();
			logger.debug("executing: Mama.close ...");
			Mama.close();
			logger.debug("executing: publisherExecutor.shutdown() ...");
			publisherExecutor.shutdown();
		} catch (NullPointerException e) {
			logger.error("NullPointerException stopping publisher: ", e);
		} catch (Exception e) {			
			logger.error("Exception stopping: ", name ,e);		
		}
	}

}
