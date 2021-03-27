package common;

import java.io.*;
import java.util.*;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable
{
	// represent a path internally as an array of path components.
	private ArrayList<String> components;
	
    /** Creates a new path which represents the root directory. */
    public Path()
    {
        this.components = new ArrayList<String>();
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
    public Path(Path path, String component)
    {
        if (component.length() == 0 || component.contains("/") || component.contains(":")) {
        	throw new IllegalArgumentException();
        }
        this.components = new ArrayList<String>();
        this.components.addAll(path.components);
        this.components.add(component);
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {
        if (path.length() == 0 || path.charAt(0) != '/' || path.contains(":")) {
        	throw new IllegalArgumentException();
        }
        this.components = new ArrayList<String>();
        for (String s : path.split("/")) {
        	if (s.length() != 0) {
            	this.components.add(s);
        	}
        }
    }
    
    public Path(ArrayList<String> list) {
    	this.components = list;
    }

    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
        class PathIterator implements Iterator<String> {
        	Iterator<String> iter;
        	
        	@SuppressWarnings("unused")
			public PathIterator() {
        		this.iter = components.iterator();
        	}
        	
        	@Override
        	public boolean hasNext() {
        		return this.iter.hasNext();
        	}
        	
        	@Override
        	public String next() {
        		return this.iter.next();
        	}
        	
        	@Override
        	public void remove() {
        		throw new UnsupportedOperationException();
        	}
        }
        
        return new PathIterator();
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException
    {
        ArrayList<Path> result = new ArrayList<Path>();
        Path.path_traversal(directory, new Path(), result);
        return result.toArray(new Path[0]);
    }
    
    private static void path_traversal(File directory, Path curr_path, ArrayList<Path> pathList) throws FileNotFoundException {
    	if (!directory.exists()) throw new FileNotFoundException();
    	if (!directory.isDirectory()) throw new IllegalArgumentException();
    	File[] files = directory.listFiles();
    	for (File file : files) {
    		if (file.isFile()) {
    			pathList.add(new Path(curr_path, file.getName()));
    		}
    		else {
    			path_traversal(file, new Path(curr_path, file.getName()), pathList);
    		}
    	}
    }

    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
        return this.components.isEmpty();
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
        if (this.isRoot()) throw new IllegalArgumentException();
        ArrayList<String> result = new ArrayList<String>();
        result.addAll(this.components);
        result.remove(result.size() - 1);
        return new Path(result);
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
    	if (this.isRoot()) throw new IllegalArgumentException();
    	return this.components.get(this.components.size() - 1);
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if it is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
    	if (this.components_size() < other.components_size()) return false;
    	
        Iterator<String> iter_this = this.iterator();
        Iterator<String> iter_other = other.iterator();
        while (iter_other.hasNext()) {
        	if (!iter_this.next().equals(iter_other.next())) return false;
        }
        return true;
    }
    
    public int components_size() {
    	return this.components.size();
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
        return new File(root.getPath() + this.toString());
    }

    /** Compares this path to another.

        <p>
        An ordering upon <code>Path</code> objects is provided to prevent
        deadlocks between applications that need to lock multiple filesystem
        objects simultaneously. By convention, paths that need to be locked
        simultaneously are locked in increasing order.

        <p>
        Because locking a path requires locking every component along the path,
        the order is not arbitrary. For example, suppose the paths were ordered
        first by length, so that <code>/etc</code> precedes
        <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.

        <p>
        Now, suppose two users are running two applications, such as two
        instances of <code>cp</code>. One needs to work with <code>/etc</code>
        and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
        <code>/etc/dfs/conf.txt</code>.

        <p>
        Then, if both applications follow the convention and lock paths in
        increasing order, the following situation can occur: the first
        application locks <code>/etc</code>. The second application locks
        <code>/bin/cat</code>. The first application tries to lock
        <code>/bin/cat</code> also, but gets blocked because the second
        application holds the lock. Now, the second application tries to lock
        <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
        need to acquire the lock for <code>/etc</code> to do so. The two
        applications are now deadlocked.

        @param other The other path.
        @return Zero if the two paths are equal, a negative number if this path
                precedes the other path, or a positive number if this path
                follows the other path.
     */
    @Override
    public int compareTo(Path other)
    {
    	// TODO:
    	// double check this implementation, am I right?
    	// instead of compare by length, we should compare two paths lexicographically
        String pathString_this = this.toString();
        String pathString_other = other.toString();
        return pathString_this.compareTo(pathString_other);
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
    	if (this.components_size() != ((Path)other).components_size()) return false;
    	
        Iterator<String> iter_this = this.iterator();
        Iterator<String> iter_other = ((Path)other).iterator();
        while (iter_other.hasNext()) {
        	if (!iter_this.next().equals(iter_other.next())) return false;
        }
        return true;
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
        int hashcode = 1;
        for (String s : this.components) {
        	hashcode = 31 * hashcode + (s == null ? 0 : s.hashCode());
        }
        return hashcode;
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
    public String toString()
    {
        if (this.isRoot()) return "/";
        String result = "";
        for (String s : this.components) {
        	result = result + "/" + s; 
        }
        return result;
    }
}
