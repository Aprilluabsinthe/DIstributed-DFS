package naming;


public class NamingServer implements Registration, Service {

    private int servicePort;
    private int registrationPort;

    public NamingServer(int servicePort, int registrationPort) {
        this.servicePort = servicePort;
        this.registrationPort = registrationPort;
    }

    @Override
    public FilesReturn Register(String storage_ip, int client_port, int command_port, String[] files)
            throws IllegalStateException {

    }

    @Override
    public Boolean IsValidPath(Path path) {

    }

    @Override
    public ServerInfo GetStorage(Path path) throws FileNotFoundException,IllegalArgumentException {

    }

    @Override
    public Boolean Delete(Path path) throws FileNotFoundException,IllegalArgumentException {

    }

    @Override
    public BooleanReturn CreateDirectory(Path path) throws FileNotFoundException,IllegalArgumentException {

    }

    @Override
    public Boolean CreateFile(Path path) throws FileNotFoundException,IllegalArgumentException,IllegalStateException {

    }

    @Override
    public FilesReturn List(Path path) throws FileNotFoundException,IllegalArgumentException {

    }

    @Override
    public Boolean IsDirectory(Path path) throws FileNotFoundException,IllegalArgumentException {

    }

    @Override
    public void Unlock(Path path, boolean exclusive) throws IllegalArgumentException {

    }

    @Override
    public void Lock(Path path, boolean exclusive) throws FileNotFoundException,IllegalArgumentException {

    }
}