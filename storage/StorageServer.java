package storage;
//import common.*;
import java.io.*;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.concurrent.ConcurrentHashMap;
import java.io.FileNotFoundException;
import java.nio.*;

public class StorageServer implements Command, Storage {

    private int clientPort;
    private int commandPort;
    private int registrationPort;
    private String rootPath;

    public StorageServer(int clientPort, int commandPort, int registrationPort, String rootPath) {
        this.clientPort = clientPort;
        this.commandPort = commandPort;
        this.registrationPort = registrationPort;
        this.rootPath = rootPath;
    }

    /**
     * Returns the length of a file, in bytes.
     * @param path
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     */
    @Override
    public long Size(String path) throws FileNotFoundException, IllegalArgumentException {
        long res = -1;

        try {
            Path pathToQuery = Paths.get(path);

            if (Files.isDirectory(pathToQuery) || !Files.exist(pathToQuery)) {
                throw FileNotFoundException("file not found");
            }

            try {
                res = Files.size(pathToQuery);
            } catch (IOException ioe) {
                ioe.printStackTrace();
                return -1;
            }
        } catch (InvalidPathException ipe) {
            throw new IllegalArgumentException("invalid path");
        }
        return res;
    }

    /**
     * Reads a sequence of bytes from a file.
     * @return
     */
    @Override
    public byte[] Read(String path, int offset, int length)
            throws FileNotFoundException,IndexOutOfBoundsException,IOException,IllegalArgumentException {

        byte[] b = new byte[offset + length];
        try {
            Path pathToRead = Paths.get(path);
            if (!Files.isReadable(pathToRead) || Files.isDirectory(pathToRead)) {
                throw new FileNotFoundException("file not found");
            }

            try {
                InputStream inputStream = Files.newInputStream(pathToRead);
                int ret = inputStream.read(b, offset, length);

            } catch (IOException ioe) {
                throw new IOException("IO exception");
            } catch (IndexOutOfBoundsException ioobe) {
                throw new IndexOutOfBoundsException("index out of bound")
            }
        } catch (InvalidPathException ipe) {
            throw new IllegalArgumentException("invalid path");
        }

        byte[] byteArr = new byte[length];
        for (int i = 0; i < byteArr.length; ++i) {
            byteArr[i] = b[offset + i];
        }
        return Base64.getEncoder().encode(byteArr);
    }

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
    @Override
    public boolean Write(String path, int offset, String data)
            throws FileNotFoundException,IndexOutOfBoundsException,IOException,IllegalArgumentException {

        try {
            Path pathToWrite = Paths.get(path);

            if (!Files.isWritable(pathToWrite) || Files.isDirectory(pathToWrite)) {
                throw new FileNotFoundException("file not found");
            }

            try {
                OutputStream outputStream = Files.newOutputStream(pathToWrite);
                byte[] writeData = Base64.getDecoder().decode(data);

                outputStream.write(writeData, offset, );

            } catch (IOException ioe) {
                throw new IOException("IO exception");
            } catch (IndexOutOfBoundsException ioobe) {
                throw new IndexOutOfBoundsException("index out of bound");
            }

        } catch (InvalidPathException ipe) {
            throw new IllegalArgumentException("invalid path");
        }
    }

    /**
     * Creates a file on the storage server.
     * @param path Path to the file to be created. The parent directory will be created if it does not exist. This path may not be the root directory.
     * @return 'true' if the file is created; 'false' if it cannot be create.
     * @throws IllegalArgumentException If the path is invalid.
     */
    @Override
    public boolean Create(String path) throws IllegalArgumentException {

        try {
            Path pathToCreate = Paths.get(path);
            if (pathToCreate.equals(pathToCreate.getRoot())) {
                return false;
            }

            Path parent = pathToCreate.getParent();

            try {
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.createFile(pathToCreate);

            } catch (IOException ioe | FileAlreadyExistsException faee) {
                return false;
            }

        } catch (InvalidPathException ipe) {
            throw new IllegalArgumentException("invalid path");
        }

        return true;
    }

    /**
     * Deletes a file or directory on the storage server.
     * @param path Path to the file or directory to be deleted. The root directory cannot be deleted.
     * @return `true` if the file is deleted; `false` if it cannot be deleted.
     * @throws IllegalArgumentException If the path is invalid
     */
    @Override
    public boolean Delete(String path) throws IllegalArgumentException {
        try {
            Path pathToDelete = Paths.get(path);

            if (pathToDelete.equals(pathToDelete.getRoot())) {
                return false;
            }

            // clean up directory if not empty
            if (Files.isDirectory(pathToDelete)) {
                File dir = new File(path);
                clearDir(dir);
            }

            try {
                Files.delete(pathToDelete);
            } catch (IOException ioe | NoSuchFileException nsfe) {
                return false;
            }

        } catch (InvalidPathException ipe) {
            throw new IllegalArgumentException("invalid path");
        }
        return true;
    }

    private void clearDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file: files) {
                clearDir(file);
            }
        }
    }

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
    @Override
    public boolean Copy(String path, String server_ip, int server_port)
            throws FileNotFoundException,IllegalArgumentException,IOException {

    }

    public int getClientPort() {
        return clientPort;
    }

    public int getRegistrationPort() {
        return registrationPort;
    }

    public int getCommandPort() {
        return commandPort;
    }

    public String getRootPath() {
        return rootPath;
    }
}