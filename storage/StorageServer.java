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

        this.client_skeleton = HttpServer.create(new InetSocketAddress(clientPort), 0);
        this.client_skeleton.setExecutor(Executors.newCachedThreadPool());

        this.command_skeleton = HttpServer.create(new InetSocketAddress(commandPort), 0);
        this.command_skeleton.setExecutor(Executors.newCachedThreadPool());
        skeleton_started = false;
        gson = new Gson();
        initialFiles = new ArrayList<>();

        rootDir = Paths.get(rootPath);

        // create local root directory if non-existent
        if (!Files.exists(rootDir)) {
            try {
                Files.createDirectories(rootDir);
            } catch (IOException ioe) {
                System.err.println("Unable to create local root due to IO");
                return;
            }
        }
        startStorageServer();
    }


    /**
     * start two HttpServer: storage and command skeletons
     * register the local storage server to the naming server via register request
     * delete duplicate files on local storage
     *
     */
    private synchronized void startStorageServer() throws IOException, InterruptedException {
        startSkeletons();

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
            } catch (IOException ioe) {
                System.err.println("Unable to delete dup files due to IO");
            }
        }
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
    private synchronized void startSkeletons() {
        if (skeleton_started) {
            return;
        }

        client_skeleton.start();
        command_skeleton.start();

        skeleton_started = true;

        add_client_apis();
        add_command_apis();
    }

    private void add_client_apis() {
        this.size();
        this.read();
        this.write();
    }

    private void add_command_apis() {
        this.create();
        this.delete();
        this.copy();
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

                    Path p = Paths.get(pathRequest.path);
                    Path pathToQuery = getAbsolutePath(p);

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

                    if (len < 0 || off + len > allData.length) {
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

                    if (!Files.isWritable(pathToWrite) || Files.isDirectory(pathToWrite)) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("FileNotFoundException", "file not found");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    int off = (int)writeRequest.offset;
                    String encodedData = writeRequest.data;

                    byte[] writeData = Base64.getDecoder().decode(encodedData);
                    byte[] dataArr = Files.readAllBytes(pathToWrite);

                    if (off < 0) {
                        returnCode = 404;
                        ExceptionReturn exceptionReturn = new ExceptionReturn("IndexOutOfBoundsException", "index out of bound");
                        respText = gson.toJson(exceptionReturn);
                        this.generateResponseAndClose(exchange, respText, returnCode);
                        return;
                    }

                    boolean success = false;

                    // have enough room to be written
                    if (off + writeData.length <= dataArr.length) {
                        for (int i = 0; i < writeData.length; ++i) {
                            dataArr[off + i] = writeData[i];
                        }
                        Files.write(pathToWrite, dataArr);
                        success = true;
                    }

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

    }

    public void delete() {

    }

    public void copy() {

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
}