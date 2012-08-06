package org.apache.hadoop.mapred;

import java.io.FileInputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.MRConstants;

public class ShufflePeer implements MRConstants{
  public static final Log LOG = LogFactory.getLog(ShufflePeer.class);
  
  String userName = null;
  String runAsUserName = null;
  long starttime; //this is important!!! schedule this peer to transfer data if now-starttime is long enough.!!
  private final int shuffle;
  private final String mapId;
  private final String shuffleId;
  private final String jobId;
  private final String remoteHost;
  // private final MapOutputLocation requestMapLoc;
  private HttpServletResponse response;

  private String localAddr;
  private int localPort;
  private String remoteAddr;
  private int remotePort;
  ////to add
  
  //byte[] buffer = new byte[MAX_BYTES_TO_READ];
//  OutputStream outStream = null;
  ////outStream = response.getOutputStream()
  
//  FileInputStream mapOutputIn = null;
//  long totalRead = 0;
  //long endtime
  

  public ShufflePeer(HttpServletRequest request, HttpServletResponse response) {
    this.shuffle = Integer.parseInt(request.getParameter("reduce"));
    this.mapId = request.getParameter("map");
    this.shuffleId = request.getParameter("reduce");
    this.jobId = request.getParameter("job");
    this.remoteHost = request.getRemoteHost();
    this.response = response;
    this.starttime = System.nanoTime();
    // this.requestMapLoc = request.get
    
    localAddr = request.getLocalAddr();
    localPort = request.getLocalPort();
    remoteAddr = request.getRemoteAddr();
    remotePort = request.getRemotePort();
    
    LOG.debug("~~~~~!!! add one peer, remoteHost is " + this.remoteHost + " and shuffleId is " + this.shuffle);
  }

  public IndexRecord getIndexRecord()
  {
//    IndexRecord ir = tracker.indexCache.getIndexInformation(mapId, reduce,indexFileName, runAsUserName);
    IndexRecord ir = null;
    return ir;
  }
  
  public void setResponse()
  {
//    response.setHeader(FROM_MAP_TASK, mapId);
//    response.setHeader(RAW_MAP_OUTPUT_LENGTH, Long.toString(info.rawLength));
//    response.setHeader(MAP_OUTPUT_LENGTH, Long.toString(info.partLength));
//    response.setHeader(FOR_REDUCE_TASK, Integer.toString(reduce));
//    response.setBufferSize(MAX_BYTES_TO_READ);
  }
  
  public HttpServletResponse getResponse() {
    return response;
  }

  public void setResponse(HttpServletResponse response) {
    this.response = response;
  }

  public int getIntShuffleId() {
    return shuffle;
  }

  public String getJobId() {
    return jobId;
  }

  public String getRemoteHost() {
    return remoteHost;
  }
  
  public String getMapId() {
    return mapId;
  }
  
  public String getStringShuffleId(){
    return shuffleId;
  }

  public String getLocalAddr() {
    return localAddr;
  }

  public int getLocalPort() {
    return localPort;
  }

  public String getRemoteAddr() {
    return remoteAddr;
  }

  public int getRemotePort() {
    return remotePort;
  }

//  public void setLocalAddr(String localAddr) {
//    this.localAddr = localAddr;
//  }
//
//  public void setLocalPort(int localPort) {
//    this.localPort = localPort;
//  }
//
//  public void setRemoteAddr(String remoteAddr) {
//    this.remoteAddr = remoteAddr;
//  }
//
//  public void setRemotePort(int remotePort) {
//    this.remotePort = remotePort;
//  }

}