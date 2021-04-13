package storage;

import java.io.*;
import java.util.*;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.concurrent.ConcurrentHashMap;
import java.io.FileNotFoundException;
import java.nio.file.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.google.gson.Gson;
import com.google.gson.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import jsonhelper.*;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.io.PrintStream;
import java.io.FileOutputStream;

public class StorageServer {


    /** client interface port number */
    private int clientPort;
    /** command interface port number */
    private int commandPort;
    /** naming server’s registration interface port number */
    private int registrationPort;
    /** local path to storage server’s local file directory */
    private String rootPath;

    private Path rootDir;

    /** local host IP */
    public static final String STORAGE_SERVER_IP = "localhost";
    /** storage interface */
    protected HttpServer client_skeleton;
    /** command interface */
    protected HttpServer command_skeleton;

    private boolean skeleton_started;
    protected Gson gson;

    private ArrayList<String> initialFiles;

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
     * start two HttpServer: storage and command skeletons
     * register the local storage server to the naming server via register request
     * delete duplicate files on local storage
     *
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

    public HttpResponse<String> register(String[] files) throws IOException, InterruptedException {

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

    private void getAllFiles(File dir) {
        File[] fileList = dir.listFiles();

        for(File file : fileList) {
            if(file.isFile()) {
                Path absolutePath = Paths.get(file.getPath());
                Path relativePath = getRelativePath(absolutePath);
                initialFiles.add(relativePath.toString());
            } else {
                getAllFiles(file);
            }
        }
    }

    private Path getAbsolutePath(Path path) {
        String pathStr = path.toString();

        // just in case
        if (pathStr.startsWith(rootPath)) {
            return path;
        }
        return Paths.get(rootPath + pathStr);
    }

    private Path getRelativePath(Path path) {
        String pathStr = path.toString();

        // just in case
        if (!pathStr.startsWith(rootPath)) {
            return path;
        }
        return Paths.get(pathStr.substring(rootPath.length()));
    }


    /**
     * start skeletons and register all APIs
     *
     */
    protected synchronized void startSkeletons() throws IOException {
        this.client_skeleton = HttpServer.create(new InetSocketAddress(this.STORAGE_SERVER_IP, this.clientPort), 0);
        this.client_skeleton.setExecutor(Executors.newCachedThreadPool());

        this.command_skeleton = HttpServer.create(new InetSocketAddress(this.STORAGE_SERVER_IP, this.commandPort), 0);
        this.command_skeleton.setExecutor(Executors.newCachedThreadPool());

        if (skeleton_started) {
            return;
        }

        client_skeleton.start();
        command_skeleton.start();

        System.out.println("starting http servers...");

        skeleton_started = true;
    }

    private void add_client_apis() {
        this.size();
        this.read();
        this.write();
        System.out.println("adding client apis...");
    }

    private void add_command_apis() {
        this.create();
        this.delete();
        this.copy();
        System.out.println("adding command apis...");
    }

    public void size() {
        this.client_skeleton.createContext("/storage_size", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                PathRequest pathRequest = null;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    pathRequest = gson.fromJson(isr, PathRequest.class);

                    System.out.println(pathRequest.path);

                    Path p = Paths.get(pathRequest.path);
                    Path pathToQuery = getAbsolutePath(p);

                    if (pathRequest.path == null || pathRequest.path.length() == 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    if (Files.isDirectory(pathToQuery) || !Files.exists(pathToQuery)) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("FileNotFoundException", "file not found");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

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

    public void read() {
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

                    if (readRequest.path == null || readRequest.path.length() == 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    if (!Files.isReadable(pathToRead) || Files.isDirectory(pathToRead)) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("FileNotFoundException", "file not found");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    int off = (int)readRequest.offset;
                    int len = readRequest.length;

                    byte[] allData = Files.readAllBytes(pathToRead);

                    if (len < 0 || off + len > allData.length || off < 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IndexOutOfBoundsException", "index out of bound");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    byte[] readData = new byte[len];
                    for (int i = 0; i < len; ++i) {
                        readData[i] = allData[off + i];
                    }
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

    public void write() {
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

                    if (writeRequest.path == null || writeRequest.path.length() == 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    if (!Files.isWritable(pathToWrite) || Files.isDirectory(pathToWrite)) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("FileNotFoundException", "file not found");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    long off = writeRequest.offset;
                    String encodedData = writeRequest.data;
                    byte[] writeData = Base64.getDecoder().decode(encodedData);

                    if (off < 0L) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IndexOutOfBoundsException", "index out of bound");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    // have enough room to be written
                    RandomAccessFile ras = new RandomAccessFile(new File(pathToWrite.toString()), "rwd");
                    ras.seek(off);
                    ras.write(writeData);


                    // to truncate the remaining length
                    if (writeData.length < Files.size(pathToWrite) && off == 0L) {
                        ras.setLength(writeData.length);
                    }

                    ras.close();
                    boolean success = true;

                    System.out.println("len after write: " + Files.size(pathToWrite));

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

    public void create() {
        this.command_skeleton.createContext("/storage_create", (exchange -> {
            String respText = "";
            int returnCode = 200;


            if (exchange.getRequestMethod().equals("POST")) {
                PathRequest pathRequest = null;

                boolean isCreated = true;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    pathRequest = gson.fromJson(isr, PathRequest.class);

                    if (pathRequest.path == null || pathRequest.path.length() == 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    Path p = Paths.get(pathRequest.path);
                    Path pathToCreate = getAbsolutePath(p);

                    if (!pathToCreate.equals(pathToCreate.getRoot())) {
                        Path parentDir = pathToCreate.getParent();

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

    public void delete() {
        this.command_skeleton.createContext("/storage_delete", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                PathRequest pathRequest = null;

                boolean isDeleted = true;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    pathRequest = gson.fromJson(isr, PathRequest.class);

                    if (pathRequest.path == null || pathRequest.path.length() == 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    Path p = Paths.get(pathRequest.path);
                    Path pathToDelete = getAbsolutePath(p);

                    if (!pathToDelete.toString().equals(this.rootPath)) {
                        System.out.println("delete start: " + pathToDelete.toString());
                        // clean a directory recursively first
                        if (Files.isDirectory(pathToDelete)) {
                            System.out.println("there is directory");
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

    private void clearDir(File dir) {

        File[] files = dir.listFiles();
        if (files != null) {
            for (File file: files) {
                clearDir(file);
            }
        }
        dir.delete();
    }

    public void copy() {
        this.command_skeleton.createContext("/storage_copy", (exchange -> {
            String respText = "";
            int returnCode = 200;

            if (exchange.getRequestMethod().equals("POST")) {
                CopyRequest copyRequest = null;

                boolean isCopied = true;

                try {
                    InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");
                    copyRequest = gson.fromJson(isr, CopyRequest.class);

                    if (copyRequest.path == null || copyRequest.path.length() == 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalArgumentException", "invalid path");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    String pathFrom = copyRequest.path;
                    Path pathToCopy = Paths.get(pathFrom);
                    String serverIp = copyRequest.server_ip;
                    int serverPort = copyRequest.server_port;

                    HttpResponse<String> sizeResponse = getRemoteSize(serverIp, serverPort, pathFrom);
                    ExceptionReturn potentialSizeException = gson.fromJson(sizeResponse.body(), ExceptionReturn.class);

                    if (potentialSizeException.exception_type != null) {
                        System.out.println("missing file");
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn(potentialSizeException.exception_type, "exception response from size request");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    // otherwise no exception from size return
                    long size = gson.fromJson(sizeResponse.body(), SizeReturn.class).size;
                    System.out.println("size: " + size);

                    HttpResponse<String> readResponse = readRemoteFile(serverIp, serverPort, pathFrom, (int)size);

                    ExceptionReturn potentialReadException = gson.fromJson(readResponse.body(), ExceptionReturn.class);
                    if (potentialReadException.exception_type != null) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn(potentialReadException.exception_type, "exception response from read request");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    String data = gson.fromJson(readResponse.body(), DataReturn.class).data;

                    boolean createSuccess = true;

                    if (!Files.exists(getAbsolutePath(pathToCopy))) {
                        HttpResponse<String> createResponse = createLocalFile(pathFrom);

                        ExceptionReturn potentialCreateException = gson.fromJson(createResponse.body(), ExceptionReturn.class);
                        if (potentialCreateException.exception_type != null) {
                            returnCode = 404;
                            ExceptionReturn exceptionReturn = new ExceptionReturn(potentialCreateException.exception_type, "exception response from read request");
                            respText = gson.toJson(exceptionReturn);
                            this.generateResponseAndClose(exchange, respText, returnCode);
                            return;
                        }

                        createSuccess = gson.fromJson(createResponse.body(), BooleanReturn.class).success;
                    }

                    HttpResponse<String> writeResponse = writeLocalFile(pathFrom, data);

                    ExceptionReturn potentialWriteException = gson.fromJson(writeResponse.body(), ExceptionReturn.class);

                    if (potentialWriteException.exception_type != null) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn(potentialWriteException.exception_type, "exception response from write request");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    boolean writeSuccess = gson.fromJson(writeResponse.body(), BooleanReturn.class).success;

                    if (!createSuccess || !writeSuccess) {
                        isCopied = false;
                    }

                    BooleanReturn booleanReturn = new BooleanReturn(isCopied);
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

    private void generateResponseAndClose(HttpExchange exchange, String respText, int returnCode) throws IOException {
        exchange.sendResponseHeaders(returnCode, respText.getBytes().length);
        OutputStream output = exchange.getResponseBody();
        output.write(respText.getBytes());
        output.flush();
        exchange.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        FileOutputStream f = new FileOutputStream("debug.txt");
        System.setOut(new PrintStream(f));
        StorageServer storageServer = new StorageServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
        storageServer.startStorageServer();
    }
}