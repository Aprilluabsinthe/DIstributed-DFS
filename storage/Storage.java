package storage;
import common.*;
import java.io.*;
import java.util.Base64;

public interface Storage {
    /**
     * Returns the length of a file, in bytes.
     * @param path
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     */
    public long Size(Path path)
            throws FileNotFoundException, IllegalArgumentException;

    /**
     * Reads a sequence of bytes from a file.
     * @return
     */
    public byte[] Read(Path path, long offset, long length)
            throws FileNotFoundException,IndexOutOfBoundsException,IOException,IllegalArgumentException;

    /**
     * Writes bytes to a file
     * @param path Path at which the directory is to be created.
     * @param offset Offset into the file where data is to be written.
     * @param data Base64 representation of data to be written.
     * @return
     * @throws FileNotFoundException
     * @throws IndexOutOfBoundsException
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public boolean Write(Path path, long offset, byte[] data)
            throws FileNotFoundException,IndexOutOfBoundsException,IOException,IllegalArgumentException;

}
