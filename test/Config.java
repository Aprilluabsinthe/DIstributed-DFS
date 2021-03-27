package test;

/** Configuration for running the test
 <p>
 This configuration file configures the command to start a naming server
 for naming test and several storage servers for storage test.

 NOTE: MUST have the same port number that follow port.config!!!
 We don't need to specify IP address here because it is all 'localhost' or '127.0.0.1'
 </p>
 */
public class Config {
    private static String separator = System.getProperty("path.separator", ":");

    /**
     * Command to start naming server.
     * TODO: change this string to start your own naming server.
     * Note: You must follow the specification in port.config!
     * After we run this command in the project's root directory, it should start a naming server
     * that listen on both SERVICE port and REGISTRATION port.
     * In our given example, 8080 is for SERVICE port, 8090 is for REGISTRATION port.
     * You should follow the order as the example shows.
    */
    public static final String startNaming = String.format("java -cp .%sgson-2.8.6.jar " +
            "naming.NamingServer 8080 8090", separator);

    /**
     * Command to start the first storage server.
     * TODO: change this string to start your own storage server.
     * Note: You must follow the specification in port.config!
     * After we run this command in the project's root directory, it should start a storage server
     * that listens on both CLIENT port and COMMAND port. You can choose whatever ports to listen on
     * as long as they are available.
     * It will also register itself through the naming server's REGISTRATION port.
     * This storage server will store all its files under the directory ROOT_DIR.
     * In our given example, 7000 is for CLIENT port, 7001 is for COMMAND port and 8090 is for
     * REGISTRATION port, and "/tmp/dist-systems-0" is for ROOT_DIR.
     * You should follow the order as the example shows.
    */
    public static final String startStorage0 = String.format("java -cp .%sgson-2.8.6.jar " +
            "storage.StorageServer 7000 7001 8090 /tmp/dist-systems-0", separator);

    /**
     * Command to start the second storage server.
     * TODO: change this string to start your own storage server.
     * Note: You must follow the specification in port.config!
     * After we run this command in the project's root directory, it should start a storage server
     * that listens on both CLIENT port and COMMAND port. You can choose whatever ports to listen on
     * as long as they are available.
     * It will also register itself through the naming server's REGISTRATION port.
     * This storage server will store all its files under the directory ROOT_DIR.
     * In our given example, 7010 is for CLIENT port, 7011 is for COMMAND port and 8090 is for
     * REGISTRATION port, and "/tmp/dist-systems-1" is for ROOT_DIR.
     * You should follow the order as the example shows.
    */
    public static final String startStorage1 = String.format("java -cp .%sgson-2.8.6.jar " +
            "storage.StorageServer 7010 7011 8090 /tmp/dist-systems-1", separator);
}
