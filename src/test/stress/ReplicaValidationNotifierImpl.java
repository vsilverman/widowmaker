/*
 * Copyright 2003-2013 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package test.stress;

public class
ReplicaValidationNotifierImpl
	implements ReplicaValidationNotifier
{
	protected int		docCount;
	protected int		masterDocCount;
	protected boolean	result;
	protected boolean	finished;
	protected long		createMillis;
	protected long		startMillis;

	public
	ReplicaValidationNotifierImpl()
	{
		docCount = 0;
		result = false;
		finished = false;
		createMillis = startMillis = System.currentTimeMillis();
	}

	public boolean
	getResult() {
		return result;
	}

	public boolean
	isFinished() {
		return finished;
	}

	public int
	getReplicaDocCount() {
		return docCount;
	}

	public void setReplicaDocCount(int count) {
		docCount = count;
	}
	
	public int
	getMasterDocCount() {
		return masterDocCount;
	}

	public void
	setMasterDocCount(int count) {
		masterDocCount = count;
	}

	public boolean
	getReplicaValidationPassed() {
		return result;
	}

	public void
	setReplicaValidationPassed(boolean r) {
		result = r;
	}

	public void startProcessing() {
		startMillis = System.currentTimeMillis();
	}

	public synchronized void
	waitForComplete() {
		while (!isFinished()) {
			try {
				wait();
			}
			catch (InterruptedException e) {
				// needs a way to have debug in here
				System.out.println("ReplicaValidationNotifier.waitForComplete was interrupted:  waiting again");
			}
		}
	}

	public synchronized void
	notifyReplicaValidationComplete() {
		long totalTime = System.currentTimeMillis() - createMillis;
		long runTime = System.currentTimeMillis() - startMillis;
		System.out.println("ReplicaValidationNotifier.notifyComplete:  tell the world - " + result + ", " + runTime + ", " + totalTime);
		finished = true;
		notifyAll();
	}

	public synchronized void
	notifyReplicaValidationComplete(boolean r) {
		result = r;
		notifyReplicaValidationComplete();
	}
}


