package org.apache.hadoop.mapred;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;

public class ShufflePeerManager {

  public static final Log LOG = LogFactory.getLog(ShufflePeerManager.class);
  // common
  private ArrayList<ShufflePeer> peerInfoList = new ArrayList<ShufflePeer>();
  private LocalDirAllocator lDirAlloc;
  private FileSystem rfs; // # = ((LocalFileSystem)
                          // context.getAttribute("local.file.system")).getRaw();

  private long lastCalled;// last call needScheduled() time ;
  private long shuffleConnectionTimeout;
  private long shuffleReadTimeout;

  // private JobConf jobConf;

  public ShufflePeerManager() {// JobConf conf) {
    this.lastCalled = System.nanoTime();
    // this.jobConf = conf;
    // this.shuffleConnectionTimeout = jobConf.getInt(
    // "mapreduce.reduce.shuffle.connect.timeout", 3 * 60 * 1000);
    // this.shuffleReadTimeout = jobConf.getInt(
    // "mapreduce.reduce.shuffle.read.timeout", 3 * 60 * 1000);
  }

  public void addPeer(HttpServletRequest request, HttpServletResponse response) {
    ShufflePeer spi = new ShufflePeer(request, response);
    // //TODO if peerInfoList has this key, how to deal with duplicate spi ?
    // //TODO new verify this task with only one job, how to deal with
    // several parallel jobs?
    peerInfoList.add(spi);
    sortPeerList();
    LOG.debug("_______addPeer, and peerInfoList size is " + peerInfoList.size());
  }

  // get and return first node in peerInfoList
  public ShufflePeer getPeer() {
    // ensure that array list is not null.
    return peerInfoList.remove(0);
  }

  public void sortPeerList() {
    this.sortPeerList(false);
  }

  public void sortPeerList(boolean multiJobs) {
    /*
     * Sort by order: 
     * 1. now-starttime value bigger first. 
     * 2. merge peer who need mapoutput file is same as other peer. 
     * 3. sortby jobid, reduceid.
     */
  }

  public boolean isEmpty() {
    return peerInfoList.isEmpty();
  }

  public boolean needScheduled() {
    if (isEmpty()) {
      return false;
    }
    // //TODO ~~!!! Judge
    lastCalled = System.nanoTime();

    return true;
  }

}
