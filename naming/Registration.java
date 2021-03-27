package naming;
import jsonhelper.*;


public interface Registration {
    public FilesReturn Register(String storage_ip, int client_port, int command_port, String[] files)
            throws IllegalStateException;
}
