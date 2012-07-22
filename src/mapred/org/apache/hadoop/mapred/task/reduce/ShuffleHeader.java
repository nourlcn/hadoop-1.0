////TODO read it!!!!!


package org.apache.hadoop.mapred.task.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Shuffle Header information that is sent by the TaskTracker and deciphered by
 * the Fetcher thread of Reduce task
 * 
 */

public class ShuffleHeader implements Writable
{

    /**
     * The longest possible length of task attempt id that we will accept.
     */
    private static final int MAX_ID_LENGTH = 1000;

    String mapId;
    long uncompressedLength;
    long compressedLength;
    int forReduce;

    public ShuffleHeader()
    {
    }

    public ShuffleHeader(String mapId, long compressedLength, long uncompressedLength, int forReduce)
    {
        this.mapId = mapId;
        this.compressedLength = compressedLength;
        this.uncompressedLength = uncompressedLength;
        this.forReduce = forReduce;
    }

    public void readFields(DataInput in) throws IOException
    {
        mapId = readStringSafely(in, MAX_ID_LENGTH);
        compressedLength = WritableUtils.readVLong(in);
        uncompressedLength = WritableUtils.readVLong(in);
        forReduce = WritableUtils.readVInt(in);
    }

    public void write(DataOutput out) throws IOException
    {
        Text.writeString(out, mapId);
        WritableUtils.writeVLong(out, compressedLength);
        WritableUtils.writeVLong(out, uncompressedLength);
        WritableUtils.writeVInt(out, forReduce);
    }

    /**
     * Read a string, but check it for sanity. The format consists of a vint
     * followed by the given number of bytes.
     * 
     * @param in
     *            the stream to read from
     * @param maxLength
     *            the largest acceptable length of the encoded string
     * @return the bytes as a string
     * @throws IOException
     *             if reading from the DataInput fails
     * @throws IllegalArgumentException
     *             if the encoded byte size for string is negative or larger
     *             than maxSize. Only the vint is read.
     */
    public static String readStringSafely(DataInput in, int maxLength) throws IOException,
            IllegalArgumentException
    {
        int length = WritableUtils.readVInt(in);
        if (length < 0 || length > maxLength)
        {
            throw new IllegalArgumentException("Encoded byte size for String was " + length
                    + ", which is outside of 0.." + maxLength + " range.");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        return Text.decode(bytes);
    }
}