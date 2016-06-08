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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SQLTester extends XccLoadTester {

	private QueryTestData queryTestData;
	private String cmd;

	public SQLTester(ConnectionData connData, QueryTestData queryData,
			String threadName) {
		super(connData, queryData.loadTestData, threadName);
		queryTestData = queryData;
		queryTestData.updateCollection(uniqueURI);
	}

	@Override
	protected void verifyIntervalAfterIteration(int loop) throws Exception {
		 // System.err.println("Collection: "+uniqueURI);
				ExecutorService exec = Executors
				.newFixedThreadPool(queryTestData.concurrency);
		for (int i = 0; i < queryTestData.repeat; i++) {
			for (int j = 0; j < queryTestData.queries.size(); j++) {
				cmd = "";
				/* cmd = "/space/towhid/SqlClient/trunk/mlsql -At -h localhost -p 5432 -Uadmin MedlineDB -c "; */
				cmd = "/opt/MarkLogic/bin/mlsql -At -h localhost -p 5431 -Uadmin ";
				cmd += queryTestData.queries.get(j).query.replaceAll("\n", "\'");
				cmd.replaceAll("\t", "");
				float result = Float.parseFloat(queryTestData.results.get(j));
				Runnable task = new VerificationTask(cmd, result);
				exec.execute(task);
			}
		}
		exec.shutdown();
		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			alive = false;
		}

	}

	private class VerificationTask implements Runnable {

		private String query;
		private float result;
		private String outPut = "";

		public VerificationTask(String query, float result) {
			this.query = query;
			this.result = result;
		}

		@Override
		public void run() {
			// no db replication support yet
			try {
				for (SessionHolder s : SQLTester.this.sessions) {
					Process proc = Runtime.getRuntime().exec(
							new String[] { "bash", "-c", query });
					int exitVal = proc.waitFor();
					if (exitVal == 0) { // everything works fine
						InputStreamReader isr = new InputStreamReader(
								proc.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String line = null;
						while ((line = br.readLine()) != null) {
							outPut += line;
						}
//						System.out.println(query);
						float f = Float.parseFloat(outPut);
						isr.close();
						isr = null; // force to garbage collector
						br.close();
						br = null;
						System.out.println("Given Value :" + result + "\n");
						System.out.println("Calculated Value :" + f + "\n");
						if (f != result) {
							String error = "ERROR running " + query;
							//s.runQuery("xdmp:log(\"" + error + "\")");
							System.err.println(error);
							System.err.println("Expected Value: "+result+"\nActual Value: "+f);
							System.err.println("Exiting...");
							alive = false;
						}
					} else {
						String error = "ERROR running " + query;
						System.err.println(error);
						System.err.println("ExitVal : " + exitVal);
						alive = false;
					}
					proc.destroy();
					proc = null;

				}
			} catch (Throwable t) {
				t.printStackTrace();
				alive = false;
			}
		}
	}
}
