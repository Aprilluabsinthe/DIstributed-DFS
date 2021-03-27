package naming;
import common.*;
import jsonhelper.*;
import java.io.*;

public interface Service {
    public Boolean IsValidPath(Path path);

    public ServerInfo GetStorage(Path path) throws FileNotFoundException,IllegalArgumentException;

    public Boolean Delete(Path path) throws FileNotFoundException,IllegalArgumentException;

    public BooleanReturn CreateDirectory(Path path) throws FileNotFoundException,IllegalArgumentException;

    public Boolean CreateFile(Path path) throws FileNotFoundException,IllegalArgumentException,IllegalStateException;

    public FilesReturn List(Path path) throws FileNotFoundException,IllegalArgumentException;

    public Boolean IsDirectory(Path path) throws FileNotFoundException,IllegalArgumentException;

    public void Unlock(Path path, boolean exclusive) throws IllegalArgumentException;

    public void Lock(Path path, boolean exclusive) throws FileNotFoundException,IllegalArgumentException;
}
