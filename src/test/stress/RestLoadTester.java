/*
 * Copyright 2003-2016 MarkLogic Corporation
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.marklogic.client.Transaction;
import com.marklogic.xcc.exceptions.RetryableXQueryException;

public class RestLoadTester extends RestStressTester {
    // include data class here
    protected LoadTestData loadTestData = null;
    protected boolean isLoadDir = false;
    protected int numLoaded = 0;
    protected String uniqueURI = null;
    protected String uriDelta = "";
    protected String randStr = null;
    protected String dateStr = null;

    public RestLoadTester(ConnectionData connData, LoadTestData loadData,
                        String threadName) {
    super(connData, loadData, threadName);
    loadTestData = loadData;
    setUniqueURI();
  }

  public void init() throws Exception {
    setLoadDir();
    setUniqueURI();
  }

  protected void setUniqueURI() {
    randStr = randomString(16);
    dateStr = getDateString();
    if (uniqueURI == null)
      uniqueURI = "/" + randStr + "/" + dateStr + "/" + threadName + "/";

    dateStr += threadName;
  }

  protected String getCollection() {
      return uniqueURI;
  };

  protected String getUriDelta() {
      return uriDelta;
  };

  @Override
  public void beginTransaction() throws Exception {
      throw new IllegalStateException("use beginRestTransaction");
  }
  @Override
  public void commitTransaction() throws Exception {
      throw new IllegalStateException("use commitRestTransaction");
  }
  @Override
  public void rollbackTransaction() throws Exception {
      throw new IllegalStateException("use rollbackRestTransaction");
  }


  public Object beginRestTransaction() throws Exception {
      if (loadTestData.getMultiStatement()) {
          Transaction transaction = (Transaction) getSession().beginTransaction();
          message("TransactionId: " + transaction.getTransactionId());
          return transaction;
      }
      return null;
  }

  public void commitRestTransaction(Object transaction) throws Exception {
      if (loadTestData.getMultiStatement()) {
          getSession().commitTransaction(transaction);
      }
  }

  public void rollbackRestTransaction(Object transaction) throws Exception {
      if (loadTestData.getMultiStatement()) {
          getSession().rollbackTransaction(transaction);
      }
  }

  // verification queries should always return a number
  protected void verifyLoaded(int numLoaded, List<String> uris) throws Exception {
      int count = 0;
      for ( String curURI : uris ) {
          boolean exists = getSession().docExists(curURI, null);
          count += exists ? 1 : 0;
      }
      if (count != numLoaded) {
          message("ERROR (RestLoadTester.verifyLoaded) found " + count +
              " loaded but should have found " + numLoaded + " of these URIs: " + uris);
          System.err.println("Exiting...");
          alive = false;
      }
  }

  protected void verifyInterval() throws Exception {
    //nop;
  }
  
  protected void verifyIntervalAfterIteration(int loop, boolean rollback) throws Exception {
    verifyInterval();
  }

  protected void setLoadDir() {
    if (!loadTestData.getLoadDir().equals(""))
      isLoadDir = true;
  }

  protected File[] listFiles(String path) {
    File dir = new File(path);
    ArrayList<File> files = new ArrayList<File>();
    for (File f : dir.listFiles())
      if (!f.isDirectory())
        files.add(f);
    return files.toArray(new File[0]);
  }

  protected void loadContentFromDir(boolean rollback) throws Exception {
      File[] fileList = listFiles(loadTestData.getLoadDir());
      int curBatch = 0;
      ArrayList<String> batchUris = new ArrayList<String>();
      int batchStart = 0;
      int retryCount = 0;
      int i = 0;
      int loadCount = 0;

      if (loadTestData.getLogOption().equalsIgnoreCase("DEBUG")) {
          message("About to load " + fileList.length + " documents.");
      }

      Object transaction = null;
      while (i < fileList.length && alive) {
          try {
              if (curBatch == 0) {
                  transaction = beginRestTransaction();
                  batchStart = i;
                  batchUris = new ArrayList<String>();
              }

              String collection = uniqueURI;
              String curURI = uniqueURI + uriDelta + fileList[i].getName();

              InputStream is = null;
              int offset;
              byte[] bytes;

              try {
                  is = new FileInputStream(fileList[i]);

                  // Get the size of the file
                  long length = fileList[i].length();

                  if (length > Integer.MAX_VALUE) {
                      throw new UnsupportedOperationException("File too large: " + fileList[i].getAbsolutePath());
                  }

                  // Create the byte array to hold the data
                  bytes = new byte[(int)length];

                  // Read in the bytes
                  offset = 0;
                  int numRead = 0;
                  while (offset < bytes.length
                          && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
                      offset += numRead;
                  }

                  // Ensure all the bytes have been read in
                  if (offset < bytes.length) {
                      throw new IOException("Could not completely read file "+fileList[i].getName());
                  }

                  is.close();
              } catch (IOException ioe) {
                  throw new RuntimeException("Failed to read file: " + fileList[i].getAbsolutePath());
              }

              loadCount++;

              long startTime = System.currentTimeMillis();

              putDocument(curURI, collection, bytes, transaction);
              batchUris.add(curURI);

              totalTime += System.currentTimeMillis() - startTime;

              // if the current batch size is equal to the desired batch size,
              // or if this is the last file in the directory commit or rollback
              if (++curBatch == loadTestData.getBatchSize()
                      || i + 1 == fileList.length) {
                  int numLoadedInBatch = finishBatch(batchUris, rollback, transaction);
                  verifyLoaded(numLoadedInBatch, batchUris);
                  numLoaded += numLoadedInBatch;
                  // current batch has been added to total so 0 it out
                  curBatch = 0;
                  retryCount = 0;
              }
              // throttles the work contributed by this thread
              Thread.sleep(loadTestData.getMaxSleepTime());

              // check documents in db at interval
              // if multistmt have to check at batch end
              if (numLoaded % loadTestData.getCheckInterval() == 0 && curBatch == 0
                      && !rollback)
                  verifyInterval();

              ++i;
          } catch (RetryableXQueryException e) {
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
                  message(Ei.toString());
                  Ei.printStackTrace();
              }
              curBatch = 0;
              if (++retryCount > 25) {
                  message("Retries exhausted");
                  e.printStackTrace();
                  ++i;
                  retryCount = 0;
              } else {
                  message("Retrying: " + retryCount);
                  i = batchStart;
                  Thread.sleep(1000);
              }
          } catch (Exception e) {
              // need to do something about exceptions and multistmt
              message(e.toString());
              e.printStackTrace();
              try {
                  rollbackRestTransaction(transaction);
              } catch (Exception Ei) {
                  message(Ei.toString());
                  Ei.printStackTrace();
              }
              ++i;
              curBatch = 0;
              retryCount = 0;
              totalTime = 0;
          }
      }
  }

  protected void putDocument(String uri, String collection, byte[] content, Object transaction) throws Exception {
      getSession().putDocument(uri, collection, content, transaction);
  }

  /** @return number of documents loaded */
  protected int finishBatch(List<String> uris, boolean rollback, Object transaction) throws Exception {
      if (!rollback) {
          commitRestTransaction(transaction);
          return uris.size(); // all uris already loaded
      } else {
          rollbackRestTransaction(transaction);
          return 0; // none loaded
      }
  }

  @Override
  public void runTest() {
      try {
          startRun();
          init();
          message("========\n" +
              "Starting test with uniqueURI = " + uniqueURI + "\n" +
              loadTestData.toString() + "\n" +
              "========");
          for (int i = 0; i < loadTestData.getNumOfLoops() && isAlive(); i++) {
              connect();
              if (isLoadDir) {
                  uriDelta = Integer.toString(i);
                  loadContentFromDir(loadTestData.getRollback());
              } else if (loadTestData.getGenerateQuery().length() != 0) {
                  // loadContentFromQuery();
              }
              if (alive)
                  verifyIntervalAfterIteration(i+1, loadTestData.getRollback());
              disconnect();
          }
      } catch (Exception e) {
          message(e.toString());
          e.printStackTrace();
      }
      alive = false;
      endRun();
  }
}
