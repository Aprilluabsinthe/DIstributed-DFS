package storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.Executors;

import com.google.gson.*;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import jsonhelper.BooleanReturn;
import jsonhelper.CopyRequest;
import jsonhelper.DataReturn;
import jsonhelper.ExceptionReturn;
import jsonhelper.FilesReturn;
import jsonhelper.PathRequest;
import jsonhelper.ReadRequest;
import jsonhelper.RegisterRequest;
import jsonhelper.SizeReturn;
import jsonhelper.WriteRequest;


/** 
 * This class encapsulates a storage server.
 * @author Yuan Gu
*/
public class StorageServer {


    /** client interface port number */
    private int clientPort;
    /** command interface port number */
    private int commandPort;
    /** naming server’s registration interface port number */
    private int registrationPort;
    /** local path to storage server’s local file directory */
    private String rootPath;

    /** local path in Java Path */
    private Path rootDir;

    /** local host IP */
    public static final String STORAGE_SERVER_IP = "localhost";
    /** storage interface */
    protected HttpServer client_skeleton;
    /** command interface */
    protected HttpServer command_skeleton;

    /** if interfaces have started */
    private boolean skeleton_started;
    /** Gson object */
    protected Gson gson;

    /** initial files under root directory when startup */
    private ArrayList<String> initialFiles;


    /**
     * Constructs a storage server. Initialize all global varibles.
     * @param clientPort the port that the server is listening for client's request
     * @param commandPort the port that the server is listening for naming server's request
     * @param registrationPort the port that the server uses to register itself to the naming server
     * @param rootPath the local root directory
     * @throws IOException IO exception
     * @throws InterruptedException Interrupted exception
     */
    public StorageServer(int clientPort, int commandPort, int registrationPort, String rootPath) throws IOException, InterruptedException {

        this.clientPort = clientPort;
        this.commandPort = commandPort;
        this.registrationPort = registrationPort;
        this.rootPath = rootPath;

        skeleton_started = false;
        gson = new Gson();
        initialFiles = new ArrayList<>();

        rootDir = Paths.get(rootPath);

        // create local root directory if non-existent
        if (!Files.exists(rootDir)) {
            try {
                Files.createDirectories(rootDir);
            } catch (IOException ioe) {
                System.out.println("Error...");
                System.err.println("Unable to create local root due to IO");
                return;
            }
        }
        System.out.println("local root dir: " + this.rootPath);
    }


    /**
     * Register the local storage server to the naming server via register request
     * delete duplicate files on local storage.
     * Start two HttpServer: storage and command skeletons.
     * Register all storage and command APIs to skeletons.
     * 
     * @throws IOException IO exception
     * @throws InterruptedException Interrupted exception
     */
    public synchronized void startStorageServer() throws IOException, InterruptedException {
        System.out.println("start storage server...");

        getAllFiles(new File(rootPath));

        String[] files = new String[initialFiles.size()];

        for (int i = 0; i < files.length; ++i) {
            files[i] = initialFiles.get(i);
        }

        // register to the naming server
        HttpResponse<String> res = register(files);

        files = gson.fromJson(res.body(), FilesReturn.class).files;

        Path[] deleted_files = new Path[files.length];

        for (int i = 0; i < deleted_files.length; ++i) {
            deleted_files[i] = Paths.get(files[i]);
        }

        /** delete all duplicates file on local storage server */
        for (Path deletedFile: deleted_files) {
            Path pathToDelete = getAbsolutePath(deletedFile);

            try {
                Files.delete(pathToDelete);

                while (pathToDelete.getParent() != null) {
                    Path pathParent = pathToDelete.getParent();
                    File dir = new File(pathParent.toString());

                    File[] fileList = dir.listFiles();
                    if (fileList.length > 0) {
                        break;
                    }
                    // prune the empty directory
                    dir.delete();
                    pathToDelete = pathParent;
                }
            } catch (IOException ioe) {
                System.err.println("Unable to delete dup files due to IO");
            }
        }
        startSkeletons();
        this.add_client_apis();
        this.add_command_apis();
        System.out.println("skeleton started");
    }

    /**
     * Wrapper function to register the local server to the naming server.
     * @param files files under the local root directory
     * @throws IOException IO exception
     * @throws InterruptedException interrupted exception
     * @return the HTTP response from naming server's register service
     */
    private HttpResponse<String> register(String[] files) throws IOException, InterruptedException {

        RegisterRequest registerRequest = new RegisterRequest(STORAGE_SERVER_IP, clientPort, commandPort, files);
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + registrationPort + "/register"))
                .setHeader("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(registerRequest)))
                .build();

        HttpResponse<String> response;

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }


    /**
     * start skeletons and register all APIs.
     * @throws IOException IO exception
     */
    private synchronized void startSkeletons() throws IOException {
        this.client_skeleton = HttpServer.create(new InetSocketAddress(STORAGE_SERVER_IP, this.clientPort), 0);
        this.client_skeleton.setExecutor(Executors.newCachedThreadPool());

        this.command_skeleton = HttpServer.create(new InetSocketAddress(STORAGE_SERVER_IP, this.commandPort), 0);
        this.command_skeleton.setExecutor(Executors.newCachedThreadPool());

        if (skeleton_started) {
            return;
        }

        client_skeleton.start();
        command_skeleton.start();

        System.out.println("starting http servers...");

        skeleton_started = true;
    }


    /**
     * Add client APIs
     */
    private void add_client_apis() {
        this.size();
        this.read();
        this.write();
        System.out.println("adding client apis...");
    }

    /**
     * Add command APIs
     */
    private void add_command_apis() {
        this.create();
        this.delete();
        this.copy();
        System.out.println("adding command apis...");
    }


    /**
     * Size request handler
     */
    private void size() {
        this.client_skeleton.createContext("/storage_size", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                PathRequest pathRequest = null;

                try {
                    // read path request
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    pathRequest = gson.fromJson(isr, PathRequest.class);

                    Path p = Paths.get(pathRequest.path);
                    Path pathToQuery = getAbsolutePath(p);

                    // if path is null or empty
                    if (pathRequest.path == null || pathRequest.path.length() == 0) {
                        this.generateExceptionResponse(exchange, respText, "IllegalArgumentException", "invalid path");
                        return;
                    }

                    // if path is a directory or doesn't exist
                    if (Files.isDirectory(pathToQuery) || !Files.exists(pathToQuery)) {
                        this.generateExceptionResponse(exchange, respText, "FileNotFoundException", "file not found");
                        return;
                    }

                    // get the size and send it back
                    long res = Files.size(pathToQuery);
                    SizeReturn sizeReturn = new SizeReturn(res);
                    returnCode = 200;
                    respText = gson.toJson(sizeReturn);

                } catch (JsonIOException | JsonSyntaxException e) {
                    respText = "Error during parse JSON object!\n";
                    returnCode = 400;
                } catch (InvalidPathException ipe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                    respText = gson.toJson(exceptionReturn);
                } catch (IOException ioe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IOException", "IO error");
                    respText = gson.toJson(exceptionReturn);
                }
            } else {
                returnCode = 400;
                respText = "Bad request method!\n";
            }

            this.generateResponseAndClose(exchange, respText, returnCode);
        }));
    }


    /**
     * Read request handler.
     */
    private void read() {
        this.client_skeleton.createContext("/storage_read", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                ReadRequest readRequest = null;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    readRequest = gson.fromJson(isr, ReadRequest.class);

                    Path p = Paths.get(readRequest.path);
                    Path pathToRead = getAbsolutePath(p);

                    // if the path is empty or null
                    if (readRequest.path == null || readRequest.path.length() == 0) {
                        this.generateExceptionResponse(exchange, respText, "IllegalArgumentException", "invalid path");
                        return;
                    }

                    // if the file doesn't exist or can't be read
                    if (!Files.isReadable(pathToRead) || Files.isDirectory(pathToRead)) {
                        this.generateExceptionResponse(exchange, respText, "FileNotFoundException", "file not found");
                        return;
                    }

                    int off = (int)readRequest.offset;
                    int len = readRequest.length;

                    byte[] allData = Files.readAllBytes(pathToRead);

                    // if the read length is negative, offset is negative, or read beyond the file's length
                    if (len < 0 || off + len > allData.length || off < 0) {
                        this.generateExceptionResponse(exchange, respText, "IndexOutOfBoundsException", "index out of bound");
                        return;
                    }

                    // read from offset and read len bytes to the result
                    byte[] readData = new byte[len];
                    for (int i = 0; i < len; ++i) {
                        readData[i] = allData[off + i];
                    }

                    // send encoded data back
                    String encodedString = Base64.getEncoder().encodeToString(readData);
                    returnCode = 200;
                    DataReturn dataReturn = new DataReturn(encodedString);
                    respText = gson.toJson(dataReturn);

                } catch (JsonIOException | JsonSyntaxException e) {
                    respText = "Error during parse JSON object!\n";
                    returnCode = 400;
                } catch (InvalidPathException ipe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                    respText = gson.toJson(exceptionReturn);
                } catch (IOException ioe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IOException", "io read error");
                    respText = gson.toJson(exceptionReturn);
                }
            } else {
                returnCode = 400;
                respText = "Bad request method!\n";
            }

            this.generateResponseAndClose(exchange, respText, returnCode);
        }));
    }

    /**
     * Write request handler.
     */
    private void write() {
        this.client_skeleton.createContext("/storage_write", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                WriteRequest writeRequest = null;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    writeRequest = gson.fromJson(isr, WriteRequest.class);

                    Path p = Paths.get(writeRequest.path);
                    Path pathToWrite = getAbsolutePath(p);

                    // if the path is null or empty
                    if (writeRequest.path == null || writeRequest.path.length() == 0) {
                        this.generateExceptionResponse(exchange, respText, "IllegalArgumentException", "invalid path");
                        return;
                    }

                    // if the file doesn't exist or can't be written
                    if (!Files.isWritable(pathToWrite) || Files.isDirectory(pathToWrite)) {
                        this.generateExceptionResponse(exchange, respText, "FileNotFoundException", "file not found");
                        return;
                    }

                    long off = writeRequest.offset;
                    String encodedData = writeRequest.data;
                    byte[] writeData = Base64.getDecoder().decode(encodedData);

                    // if offset is negative, return index out of bound exception
                    if (off < 0L) {
                        this.generateExceptionResponse(exchange, respText, "IndexOutOfBoundsException", "index out of bound");
                        return;
                    }

                    // seek to the offset and start writing all data
                    RandomAccessFile ras = new RandomAccessFile(new File(pathToWrite.toString()), "rwd");
                    ras.seek(off);
                    ras.write(writeData);

                    // to truncate the remaining length if the new data is less than old data
                    // this is used in copy when we attempt to replace a file with less new data
                    if (writeData.length < Files.size(pathToWrite) && off == 0L) {
                        ras.setLength(writeData.length);
                    }

                    ras.close();
                    boolean success = true;

                    BooleanReturn booleanReturn = new BooleanReturn(success);
                    returnCode = 200;
                    respText = gson.toJson(booleanReturn);

                } catch (JsonIOException | JsonSyntaxException e) {
                    respText = "Error during parse JSON object!\n";
                    returnCode = 400;
                } catch (InvalidPathException ipe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                    respText = gson.toJson(exceptionReturn);
                } catch (IOException ioe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IOException", "io write error");
                    respText = gson.toJson(exceptionReturn);
                }
            } else {
                returnCode = 400;
                respText = "Bad request method!\n";
            }

            this.generateResponseAndClose(exchange, respText, returnCode);
        }));
    }

    /**
     * Create request handler.
     */
    private void create() {
        this.command_skeleton.createContext("/storage_create", (exchange -> {
            String respText = "";
            int returnCode = 200;


            if (exchange.getRequestMethod().equals("POST")) {
                PathRequest pathRequest = null;

                boolean isCreated = true;

                try {
                    // read path request
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    pathRequest = gson.fromJson(isr, PathRequest.class);

                    // if path is empty or null
                    if (pathRequest.path == null || pathRequest.path.length() == 0) {
                        this.generateExceptionResponse(exchange, respText, "IllegalArgumentException", "invalid path");
                        return;
                    }

                    Path p = Paths.get(pathRequest.path);
                    Path pathToCreate = getAbsolutePath(p);

                    // make sure that the path to create is not the root directory
                    if (!pathToCreate.toString().equals(this.rootPath)) {
                        Path parentDir = pathToCreate.getParent();

                        // if the parent directory doesn't exist, we create all directories first
                        if (parentDir != null) {
                            Files.createDirectories(parentDir);
                        }
                        Files.createFile(pathToCreate);
                    } else {
                        isCreated = false;
                    }

                    BooleanReturn booleanReturn = new BooleanReturn(isCreated);
                    respText = gson.toJson(booleanReturn);

                } catch (JsonIOException | JsonSyntaxException e) {
                    respText = "Error during parse JSON object!\n";
                    returnCode = 400;
                } catch (InvalidPathException ipe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                    respText = gson.toJson(exceptionReturn);
                } catch (IOException ioe) {
                    // cannot create the file due to IO error
                    isCreated = false;
                    BooleanReturn booleanReturn = new BooleanReturn(isCreated);
                    respText = gson.toJson(booleanReturn);
                }
            } else {
                returnCode = 400;
                respText = "Bad request method!\n";
            }

            this.generateResponseAndClose(exchange, respText, returnCode);
        }));
    }

    /**
     * Delete request handler.
     */
    private void delete() {
        this.command_skeleton.createContext("/storage_delete", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                PathRequest pathRequest = null;

                boolean isDeleted = true;

                try {
                    // read path request
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    pathRequest = gson.fromJson(isr, PathRequest.class);

                    // if the path is null or empty
                    if (pathRequest.path == null || pathRequest.path.length() == 0) {
                        this.generateExceptionResponse(exchange, respText, "IllegalArgumentException", "invalid path");
                        return;
                    }

                    Path p = Paths.get(pathRequest.path);
                    Path pathToDelete = getAbsolutePath(p);

                    // make sure the path is not the root directory
                    if (!pathToDelete.toString().equals(this.rootPath)) {
                        System.out.println("delete start: " + pathToDelete.toString());

                        // clear a directory recursively first
                        if (Files.isDirectory(pathToDelete)) {
                            File dir = new File(pathToDelete.toString());
                            clearDir(dir);
                        } else {
                            Files.delete(pathToDelete);
                        }
                    } else {
                        isDeleted = false;
                    }

                    BooleanReturn booleanReturn = new BooleanReturn(isDeleted);
                    respText = gson.toJson(booleanReturn);

                } catch (JsonIOException | JsonSyntaxException e) {
                    respText = "Error during parse JSON object!\n";
                    returnCode = 400;
                } catch (InvalidPathException ipe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                    respText = gson.toJson(exceptionReturn);
                } catch (IOException ioe) {
                    // cannot delete the file due to IO error
                    isDeleted = false;
                    BooleanReturn booleanReturn = new BooleanReturn(isDeleted);
                    respText = gson.toJson(booleanReturn);
                }
            } else {
                returnCode = 400;
                respText = "Bad request method!\n";
            }

            this.generateResponseAndClose(exchange, respText, returnCode);
        }));
    }

    /**
     * Copy request handler.
     */
    private void copy() {
        this.command_skeleton.createContext("/storage_copy", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                CopyRequest copyRequest = null;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    copyRequest = gson.fromJson(isr, CopyRequest.class);

                    // if the path is empty or null
                    if (copyRequest.path == null || copyRequest.path.length() == 0) {
                        this.generateExceptionResponse(exchange, respText, "IllegalArgumentException", "invalid path");
                        return;
                    }

                    String pathFrom = copyRequest.path;
                    Path pathToCopy = Paths.get(pathFrom);
                    String serverIp = copyRequest.server_ip;
                    int serverPort = copyRequest.server_port;

                    // send a size request to the remote storage server
                    HttpResponse<String> sizeResponse = getRemoteSize(serverIp, serverPort, pathFrom);
                    String sizeException = this.getPotentialException(sizeResponse);

                    if (sizeException != null) {
                        this.generateExceptionResponse(exchange, respText, sizeException, "exception response from size request");
                        return;
                    }

                    // otherwise no exception from size return, send a read request to read all content
                    long size = gson.fromJson(sizeResponse.body(), SizeReturn.class).size;
                    HttpResponse<String> readResponse = readRemoteFile(serverIp, serverPort, pathFrom, (int)size);

                    String readException = this.getPotentialException(readResponse);
                    if (readException != null) {
                        this.generateExceptionResponse(exchange, respText, readException, "exception response from read request");
                        return;
                    }

                    // no exception from read return, get the file's content
                    String data = gson.fromJson(readResponse.body(), DataReturn.class).data;

                    boolean createSuccess = true;

                    // try to create a local file if the path doesn't exist
                    if (!Files.exists(getAbsolutePath(pathToCopy))) {
                        HttpResponse<String> createResponse = createLocalFile(pathFrom);

                        String createException = this.getPotentialException(createResponse);
                        if (createException != null) {
                            this.generateExceptionResponse(exchange, respText, createException, "exception response from create request");
                            return;
                        }
                        createSuccess = gson.fromJson(createResponse.body(), BooleanReturn.class).success;
                    }

                    // write the data (read from the remote server) to the local file just created
                    HttpResponse<String> writeResponse = writeLocalFile(pathFrom, data);
                    String writeException = this.getPotentialException(writeResponse);

                    if (writeException != null) {
                        this.generateExceptionResponse(exchange, respText, writeException, "exception response from write request");
                        return;
                    }

                    boolean writeSuccess = gson.fromJson(writeResponse.body(), BooleanReturn.class).success;

                    // make sure the file is successfully created and written
                    BooleanReturn booleanReturn = new BooleanReturn(createSuccess && writeSuccess);
                    respText = gson.toJson(booleanReturn);


                } catch (JsonIOException | JsonSyntaxException e) {
                    respText = "Error during parse JSON object!\n";
                    returnCode = 400;
                } catch (InvalidPathException ipe) {
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                    respText = gson.toJson(exceptionReturn);
                } catch (IOException | InterruptedException e) {
                    // IO on any case
                    returnCode = 404;
                    ExceptionReturn exceptionReturn = new ExceptionReturn("IOException", "IO error");
                    respText = gson.toJson(exceptionReturn);
                }
            } else {
                returnCode = 400;
                respText = "Bad request method!\n";
            }

            this.generateResponseAndClose(exchange, respText, returnCode);
        }));
    }

    /**
     * Wrapper function to get file's size from a remote server.
     * @param ip the remote server's IP
     * @param port the remote server's port
     * @param path the path
     * @return a HTTP response
     * @throws IOException IO exception
     * @throws InterruptedException interrupted exception
     */
    private HttpResponse<String> getRemoteSize(String ip, int port, String path) throws IOException, InterruptedException {
        PathRequest pathRequest = new PathRequest(path);

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + ip + ":" + port + "/storage_size"))
                .setHeader("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(pathRequest)))
                .build();

        HttpResponse<String> response;

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }
    
    /**
     * Wrapper function to read all content of a file from a remote server.
     * @param ip the remote server's IP
     * @param port the remote server's port
     * @param path the path to the file
     * @param len the size of the file
     * @return a HTTP response
     * @throws IOException IO exception
     * @throws InterruptedException interrupted exception
     */
    private HttpResponse<String> readRemoteFile(String ip, int port, String path, int len) throws IOException, InterruptedException {
        ReadRequest readRequest = new ReadRequest(path, 0, len);

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + ip + ":" + port + "/storage_read"))
                .setHeader("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(readRequest)))
                .build();

        HttpResponse<String> response;

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }

    /**
     * Wrapper function to create a file in the local server.
     * @param path the path to the file
     * @return a HTTP response
     * @throws IOException IO exception
     * @throws InterruptedException interrupted exception
     */
    private HttpResponse<String> createLocalFile(String path) throws IOException, InterruptedException {
        PathRequest pathRequest = new PathRequest(path);
        System.out.println("local: " + path);

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + this.commandPort + "/storage_create"))
                .setHeader("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(pathRequest)))
                .build();

        HttpResponse<String> response;

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }

    /**
     * Wrapper function to write a file in the local server.
     * @param path the path to the file
     * @param writeData the data to be written
     * @return a HTTP response
     * @throws IOException IO exception
     * @throws InterruptedException interrupted exception
     */
    private HttpResponse<String> writeLocalFile(String path, String writeData) throws IOException, InterruptedException {
        WriteRequest writeRequest = new WriteRequest(path, 0, writeData);

        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + this.clientPort + "/storage_write"))
                .setHeader("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(writeRequest)))
                .build();

        HttpResponse<String> response;

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }


    /**
     * Get client port.
     * @return the client port
     */
    public int getClientPort() {
        return clientPort;
    }

    /**
     * Get registration port.
     * @return the registration port
     */
    public int getRegistrationPort() {
        return registrationPort;
    }

    /**
     * Get command port.
     * @return the command port
     */
    public int getCommandPort() {
        return commandPort;
    }

    /**
     * Get the local root directory.
     * @return the directory
     */
    public String getRootPath() {
        return rootPath;
    }

    /**
     * Generate a exception response to the client.
     * @param exchange the HTTP exchange
     * @param respText the response text
     * @param exceptionType the exception type
     * @param exceptionInfo the exception message
     * @throws IOException IO exception
     */
    private void generateExceptionResponse(HttpExchange exchange, String respText, String exceptionType, String exceptionInfo) throws IOException {
        ExceptionReturn exceptionReturn = new ExceptionReturn(exceptionType, exceptionInfo);
        respText = gson.toJson(exceptionReturn);
        this.generateResponseAndClose(exchange, respText, 404);
    }

    /**
     * Generate a HTTP response to the client and close the exchange context.
     * @param exchange the HTTP exchange
     * @param respText the response text
     * @param returnCode the return code
     * @throws IOException IO exception
     */
    private void generateResponseAndClose(HttpExchange exchange, String respText, int returnCode) throws IOException {
        exchange.sendResponseHeaders(returnCode, respText.getBytes().length);
        OutputStream output = exchange.getResponseBody();
        output.write(respText.getBytes());
        output.flush();
        exchange.close();
    }

    /**
     * Determine whether a HTTP response is an exception return.
     * @param response the HTTP response
     * @return the exception type if the response is an exception, null otherwise
     */
    private String getPotentialException(HttpResponse<String> response) {
        ExceptionReturn potentialException = gson.fromJson(response.body(), ExceptionReturn.class);
        return potentialException.exception_type;
    }

    /**
     * Helper function to recursively delete a directory.
     * @param dir the directory to be deleted
     */
    private void clearDir(File dir) {

        File[] files = dir.listFiles();
        if (files != null) {
            for (File file: files) {
                clearDir(file);
            }
        }
        dir.delete();
    }
    
    /**
     * Helper function to recursively get all files under the root directory.
     * @param dir the local root directory
     */
    private void getAllFiles(File dir) {
        File[] fileList = dir.listFiles();

        for(File file : fileList) {
            if(file.isFile()) {
                Path absolutePath = Paths.get(file.getPath());
                Path relativePath = getRelativePath(absolutePath);
                // we only want the naming server know the relative path
                initialFiles.add(relativePath.toString());
            } else {
                getAllFiles(file);
            }
        }
    }

    /**
     * Convert a relative path to the absolute path.
     * @param path the relative path
     * @return the absolute path
     */
    private Path getAbsolutePath(Path path) {
        String pathStr = path.toString();

        // just in case
        if (pathStr.startsWith(rootPath)) {
            return path;
        }
        return Paths.get(rootPath + pathStr);
    }

    /**
     * Convert a absolute path to the relative path
     * @param path the absolute path
     * @return the relative path
     */
    private Path getRelativePath(Path path) {
        String pathStr = path.toString();

        // just in case
        if (!pathStr.startsWith(rootPath)) {
            return path;
        }
        return Paths.get(pathStr.substring(rootPath.length()));
    }

    /**
     * Main driver to start the storage server and redirect all print lines to "debug.txt" file.
     * @param args arguments to be read
     * @throws IOException IO exception
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        FileOutputStream f = new FileOutputStream("debug.txt");
        System.setOut(new PrintStream(f));
        StorageServer storageServer = new StorageServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
        storageServer.startStorageServer();
    }
}