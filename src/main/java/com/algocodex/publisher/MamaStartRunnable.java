package com.algocodex.publisher;

import com.wombat.mama.Mama;
import com.wombat.mama.MamaBridge;

public class MamaStartRunnable implements Runnable {
		
	private final MamaBridge bridge;
	private boolean completed = false;
	
	public MamaStartRunnable(MamaBridge bridge) {
		this.bridge = bridge;
	}

	public void run() {	
		Mama.start(bridge);
		completed = true;
	}

	public boolean hasCompleted() {
		return completed;
	}
}
