////split reader into class.
////TODO: READ IT

package org.apache.hadoop.mapred.task.reduce;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RamManager;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.IFile.Reader;

/**
 * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
 */

public class InMemoryReader<K, V> extends Reader<K, V> {

  RamManager ramManager;
  TaskAttemptID taskAttemptId;
  
  private static final int EOF_MARKER = -1;

  public InMemoryReader(RamManager ramManager, TaskAttemptID taskAttemptId,
      byte[] data, int start, int length) throws IOException {
    super(null, null, length - start, null, null);
    this.ramManager = ramManager;
    this.taskAttemptId = taskAttemptId;

    buffer = data;
    bufferSize = (int) fileLength;
    dataIn.reset(buffer, start, length);
  }

  @Override
  public long getPosition() throws IOException {
    // InMemoryReader does not initialize streams like Reader, so in.getPos()
    // would not work. Instead, return the number of uncompressed bytes read,
    // which will be correct since in-memory data is not compressed.
    return bytesRead;
  }

  @Override
  public long getLength() {
    return fileLength;
  }

  private void dumpOnError() {
    File dumpFile = new File("../output/" + taskAttemptId + ".dump");
    System.err.println("Dumping corrupt map-output of " + taskAttemptId
        + " to " + dumpFile.getAbsolutePath());
    try {
      FileOutputStream fos = new FileOutputStream(dumpFile);
      fos.write(buffer, 0, bufferSize);
      fos.close();
    } catch (IOException ioe) {
      System.err.println("Failed to dump map-output of " + taskAttemptId);
    }
  }

  public boolean next(DataInputBuffer key, DataInputBuffer value)
      throws IOException {
    try {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }

      // Read key and value lengths
      int oldPos = dataIn.getPosition();
      int keyLength = WritableUtils.readVInt(dataIn);
      int valueLength = WritableUtils.readVInt(dataIn);
      int pos = dataIn.getPosition();
      bytesRead += pos - oldPos;

      // Check for EOF
      if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (keyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: "
            + keyLength);
      }
      if (valueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: "
            + valueLength);
      }

      final int recordLength = keyLength + valueLength;

      // Setup the key and value
      pos = dataIn.getPosition();
      byte[] data = dataIn.getData();
      key.reset(data, pos, keyLength);
      value.reset(data, (pos + keyLength), valueLength);

      // Position for the next record
      long skipped = dataIn.skip(recordLength);
      if (skipped != recordLength) {
        throw new IOException("Rec# " + recNo
            + ": Failed to skip past record of length: " + recordLength);
      }

      // Record the byte
      bytesRead += recordLength;

      ++recNo;

      return true;
    } catch (IOException ioe) {
      dumpOnError();
      throw ioe;
    }
  }

  public void close() {
    // Release
    dataIn = null;
    buffer = null;

    // Inform the RamManager
    ramManager.unreserve(bufferSize);
  }

}

// ////_____________bellow: wuhan_hadoop version

// {
// private final TaskAttemptID taskAttemptId;
// private final MergeManager<K, V> merger;
// // DataInputBuffer memDataIn = new DataInputBuffer();
// private int start;
// private int length;
//
// public InMemoryReader(MergeManager<K, V> merger, TaskAttemptID taskAttemptId,
// byte[] data,
// int start, int length) throws IOException
// {
// super(null, null, length - start, null, null);
// this.merger = merger;
// this.taskAttemptId = taskAttemptId;
//
// buffer = data;
// bufferSize = (int) fileLength;
// dataIn.reset(buffer, start, length);
// this.start = start;
// this.length = length;
// }
//
// @Override
// public void reset(int offset)
// {
// dataIn.reset(buffer, start + offset, length);
// bytesRead = offset;
// eof = false;
// }
//
// @Override
// public long getPosition() throws IOException
// {
// // InMemoryReader does not initialize streams like Reader, so
// // in.getPos()
// // would not work. Instead, return the number of uncompressed bytes
// // read,
// // which will be correct since in-memory data is not compressed.
// return bytesRead;
// }
//
// @Override
// public long getLength()
// {
// return fileLength;
// }
//
// private void dumpOnError()
// {
// File dumpFile = new File("../output/" + taskAttemptId + ".dump");
// System.err.println("Dumping corrupt map-output of " + taskAttemptId + " to "
// + dumpFile.getAbsolutePath());
// try
// {
// FileOutputStream fos = new FileOutputStream(dumpFile);
// fos.write(buffer, 0, bufferSize);
// fos.close();
// }
// catch (IOException ioe)
// {
// System.err.println("Failed to dump map-output of " + taskAttemptId);
// }
// }
//
// @Override
// public boolean nextRawKey(DataInputBuffer key) throws IOException
// {
// try
// {
// return super.nextRawKey(key);
// }
// catch (IOException ioe)
// {
// dumpOnError();
// throw ioe;
// }
// }
//
// @Override
// public void nextRawValue(DataInputBuffer value) throws IOException
// {
//
// try
// {
// super.nextRawValue(value);
// }
// catch (IOException ioe)
// {
// dumpOnError();
// throw ioe;
// }
// }
//
// @Override
// public void close()
// {
// // Release
// dataIn = null;
// buffer = null;
// // Inform the MergeManager
// if (merger != null)
// {
// merger.unreserve(bufferSize);
// }
// }
//
// /**
// * writing (dumping) byte-array data to output stream.
// */
// @Override
// public void dumpTo(IStreamWriter writer, Progressable progressable,
// Configuration conf)
// throws IOException
// {
// writer.write(buffer, start, length - IFile.LEN_OF_EOF);
//
// DataInputBuffer din = new DataInputBuffer();
// din.reset(buffer, start + length - IFile.LEN_OF_EOF, IFile.LEN_OF_EOF);
// verifyEOF(din);
// }
// }
