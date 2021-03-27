package storage;
import common.*;
import java.io.*;

public interface Command {
    /**
     * Creates a file on the storage server.
     * @param path Path to the file to be created. The parent directory will be created if it does not exist. This path may not be the root directory.
     * @return 'true' if the file is created; 'false' if it cannot be create.
     * @throws IllegalArgumentException If the path is invalid.
     */
    public boolean Create(Path path) throws IllegalArgumentException;

    /**
     * Deletes a file or directory on the storage server.
     * @param path Path to the file or directory to be deleted. The root directory cannot be deleted.
     * @return `true` if the file is deleted; `false` if it cannot be deleted.
     * @throws IllegalArgumentException If the path is invalid
     */
    public boolean Delete(Path path) throws IllegalArgumentException;

    /**
     * Copies a file from another storage server.
     * @param path Path to the file to be copied.
     * @param server_ip IP of the storage server that hosting the file.
     * @param server_port storage port of the storage server that hosting the file.
     * @return `true` if the file is successfully copied; `false` otherwise.
     * @throws FileNotFoundException If the file cannot be found or the path refers to a directory
     * @throws IllegalArgumentException If the path is invalid
     * @throws IOException If an I/O exception occurs either on the remote or on this storage server.
     */
    public boolean Copy(Path path, String server_ip, int server_port)
            throws FileNotFoundException,IllegalArgumentException,IOException;

}
