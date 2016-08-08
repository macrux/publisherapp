package com.algocodex.publisher;

import java.util.Scanner;
import java.util.logging.Level;

import com.wombat.mama.MamaMsg;

public class MsgPublisher {

	private static MamaTopicPublisher publisher;

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		String op = "";

		do {
			System.out.println("Press i to start publisher\n" 
							 + "Press f to stop publisher\n"
							 + "Press p to publish message\n" 
							 + "Press q to quit app\n");

			op = scan.next();

			if (op.equalsIgnoreCase("i")) {
				if (publisher == null) {
					publisher = new MamaTopicPublisher("pubapp", "pub", "zmq", "TEST");
					publisher.setOptions(Level.FINEST, true);
					publisher.start();
					System.out.println("Publisher started with:\n" 
									 + " name:\t pubapp\n" 
							         + " broker:\t zmq\n"
							         + " topic:\t TEST\n");
				}else {
					System.out.println("*publisher already started. Press f to stop it and then press i to restart*");
				}

			} else if (op.equalsIgnoreCase("f")) {
				if (publisher != null) {
					publisher.stop();
					publisher = null;
				} else {
					System.out.println("*start publisher first (press i)*");
				}
			} else if (op.equalsIgnoreCase("p")) {
				if (publisher != null) {
					sendMsg();
				} else {
					System.out.println("*start publisher first (press i)*");
				}
			}
		} while (!op.equalsIgnoreCase("q"));

		if(publisher != null){
			publisher.stop();
		}
		scan.close();
	}

	private static void sendMsg() {
		MamaMsg myMsg = new MamaMsg();
		publisher.publishMessage(myMsg);
	}
}
