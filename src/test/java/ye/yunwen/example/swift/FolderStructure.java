package ye.yunwen.example.swift;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.logging.Level;

import ch.iterate.openstack.swift.AuthenticationResponse;
import ch.iterate.openstack.swift.Client;
import ch.iterate.openstack.swift.Client.AuthVersion;
import ch.iterate.openstack.swift.exception.ContainerExistsException;
import ch.iterate.openstack.swift.model.ObjectMetadata;
import ch.iterate.openstack.swift.model.Region;
import ch.iterate.openstack.swift.model.StorageObject;

/**
 * Demonstrate the simulated folder structure of Swift.
 * @author yunwen
 *
 */
public class FolderStructure {
	
	//private String _authUrlStr = "http://54.226.238.78/auth/v1.0";
	private String _authUrlStr = "http://54.211.239.195/auth/v1.0";
	
	private static Logger log = Logger.getLogger(FolderStructure.class.getName());
	
	/**
	 * The container (or bucket in S3 lingo). 
	 */
	private String _container = "folderdemo";
	private String _username = "test:tester";
	private String _password = "testing";
	
	private Client _swiftClient;
	private Region _region;
	private AuthenticationResponse _authResponse;
	private String _authToken;
	private AuthVersion _authVersion = AuthVersion.v10;
	
	public FolderStructure() {
		init();
	}
	
	private void init() {
		_swiftClient = new Client(600000);  //60 seconds
	}
	
	public void login() throws IOException, URISyntaxException {
		login(false);
	}
	
	public synchronized boolean login(boolean relogin) throws IOException, URISyntaxException {
		URI authUri  = new URI(_authUrlStr);
		if (relogin || _region == null || _authToken == null) {
			_authResponse = _swiftClient.authenticate(_authVersion, authUri, _username, _password, null);
			_authToken = _authResponse.getAuthToken();
			Set<Region> rs = _authResponse.getRegions();
			if (rs == null || rs.isEmpty()) {
				log.log(Level.SEVERE, "Returned region is empty");
				log.log(Level.SEVERE, "Response is: " + _authResponse.getStatusLine().toString());
				return false;
			} else {
				_region = _authResponse.getRegions().iterator().next();
			}
		} 
		return true;
	}
	
	private final String KeyAB11 = "a/b1/fb11.txt";
	private final String KeyAB12 = "a/b1/fb12.txt";
	private final String KeyAB21 = "a/b2/fb21.txt";
	private final String KeyA1 	 = "a/fa1.txt";
	private final String[] keys = new String[]{KeyAB11, KeyAB12, KeyAB21, KeyA1};
	
	private final InputStream data = new ByteArrayInputStream("testtesttest".getBytes());
	
	/**
	 * Create following files and folders
	 * 	folderdemo/a/b1/fb11.txt
	 *  folderdemo/a/b1/fb12.txt
	 *  folderdemo/a/b2/fb21.txt
	 *  folderdemo/a/fa1.txt
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public void createFiles() throws IOException, URISyntaxException {
		login(false);
		createContainerIfNotExist();
		for (String k: keys) {
			_swiftClient.storeObject(_region, _container, data, "plain/ascii", k, 
					new java.util.HashMap<String, String>());
		}
	}
	
	public void createContainerIfNotExist() throws IOException {
		try {
			_swiftClient.createContainer(_region, _container);
		} catch (ContainerExistsException ignored) {
			;
		}
	}
	
	public void list() throws IOException, URISyntaxException {
		login(false);
		String startWith = "a";
		String path = null; 
		String marker = null;
		int limit = -1;
		headInfo("List with prefix=a, expecting 4 items");
		List<StorageObject> objs = _swiftClient.listObjectsStartingWith(
				_region, _container, startWith, path, limit, marker);
		print(objs);
		
		Character delimiter = '/';
		headInfo("List with prefix=a&delimiter=/, expecting 1 items: a/");
		objs = _swiftClient.listObjectsStartingWith(
				_region, _container, startWith, path, limit, marker, delimiter);
		print(objs);
		
		startWith = "a/";
		//same as listing all sub-folders and files under a folder
		//"GET /v1/AUTH_test/folderdemo?format=xml&prefix=a%2F&delimiter=%2F HTTP/1.1[\r][\n]"
		headInfo("List with prefix=a/&delimiter=/, expecting 3 items: a/fa1.txt, a/b1/, a/b2/");
		objs = _swiftClient.listObjectsStartingWith(
				_region, _container, startWith, path, limit, marker, delimiter);
		print(objs);
		
		path = "a";
		//same as listing all files (no sub-folders) under a folder
		//"GET /v1/AUTH_test/folderdemo?format=xml&path=a HTTP/1.1[\r][\n]"
		headInfo("List with path=a, expecting 1 items: a/fa1.txt");
		objs = _swiftClient.listObjects(
				_region, _container, path, limit);
		print(objs);
		
		path = "a/b1";
		//GET /v1/AUTH_test/folderdemo?format=xml&path=a%2Fb1 HTTP/1.1[\r][\n]
		headInfo("List with path=a/b1, expecting 2 items: a/b1/fb11.txt, a/b1/fb12.txt");
		objs = _swiftClient.listObjects(
				_region, _container, path, limit);
		print(objs);
		
		path = "a/";
		//GET /v1/AUTH_test/folderdemo?format=xml&path=a%2F HTTP/1.1[\r][\n]
		headInfo("List with path=a/, expecting 1 items: a/fa1.txt");
		objs = _swiftClient.listObjects(
				_region, _container, path, limit);
		print(objs);
	}
	
	public void listMeta(String key) throws IOException, URISyntaxException {
		login();
		ObjectMetadata meta = _swiftClient.getObjectMetaData(_region, _container, key);
		System.out.println(meta);
	}
	
	public void expire(String key) throws IOException, URISyntaxException {
		login();
//	Cannot use this. All map entry is prefixed with X-Object-Mata-, which does not work with X-Delete-After
//  or X-Delete-At
//		Map<String, String> map = new HashMap<String, String>();
//		map.put("X-Delete-After", "1");
//		_swiftClient.updateObjectMetadata(_region, _container, key, map);
		_swiftClient.expireObjectAfter(_region, _container, key, 1);
		long start = System.currentTimeMillis();
		while (true) {
			try {
				ObjectMetadata meta = _swiftClient.getObjectMetaData(_region, _container, key);
				System.out.println(meta);
			} catch (ch.iterate.openstack.swift.exception.NotFoundException e) {
				System.out.println("Deleted after " + (System.currentTimeMillis() - start) + " milliseconds");
				break;
			}
		}
	}
	
	/**
	 * "DELETE /v1/AUTH_test/folderdemo?bulk-delete=1 HTTP/1.1[\r][\n]"
	   "Content-Type: text/plain[\r][\n]"
       "X-Auth-Token: AUTH_tk19f0d408ba6c415698d0f24e32417b95[\r][\n]"
       "Content-Length: 5080[\r][\n]"
       "Host: 54.211.239.195[\r][\n]"
       "Connection: Keep-Alive[\r][\n]"
       
		folderdemo/testdeleteDir%2Fkey0
		folderdemo/testdeleteDir%2Fkey1
		folderdemo/testdeleteDir%2Fkey2
		folderdemo/testdeleteDir%2Fkey3
		folderdemo/testdeleteDir%2Fkey4
		folderdemo/testdeleteDir%2Fkey5
		folderdemo/testdeleteDir%2Fkey6
		
		Performance Note: 1000 objects were deleted in 121,784 milliseconds
	*/
	public void delete() throws IOException, URISyntaxException {
		int sz = 1000;
		List<String> keys1 = new ArrayList<String>();
		List<String> keys2 = new ArrayList<String>();
		for (int i=0; i<sz; i++) {
			keys1.add("testdeleteDir/key" + i);
			keys2.add("kkey" + i);
		}
		createObjects(keys1);
		createObjects(keys2);
		
		List<String> keys3 = new ArrayList<String>();
		keys3.addAll(keys1);
		keys3.addAll(keys2);
		headInfo(String.format("Delete %d objects", sz));
		long start = System.currentTimeMillis();
		_swiftClient.deleteObjects(_region, _container, keys3);
		System.out.format("\n--Used %d millisecond\n", System.currentTimeMillis() - start);
	}
	
//	public void expire() throws IOException, URISyntaxException {
//		int sz = 1000;
//		List<String> keys1 = new ArrayList<String>();
//		List<String> keys2 = new ArrayList<String>();
//		for (int i=0; i<sz; i++) {
//			keys1.add("testdeleteDir/key" + i);
//			keys2.add("kkey" + i);
//		}
//		createObjects(keys1);
//		createObjects(keys2);
//		
//		List<String> keys3 = new ArrayList<String>();
//		keys3.addAll(keys1);
//		keys3.addAll(keys2);
//		headInfo(String.format("Delete %d objects", sz));
//		long start = System.currentTimeMillis();
//		_swiftClient.deleteObjects(_region, _container, keys3);
//		System.out.format("\n--Used %d nanoseconds\n", System.currentTimeMillis() - start);
//	}
	
	public void createObjects(List<String> keys) throws IOException, URISyntaxException {
		login();
		createContainerIfNotExist();
		for (String key: keys) {
			_swiftClient.storeObject(_region, _container, data, "plain/ascii", key, 
					new java.util.HashMap<String, String>());
		}
	}
	
	public void headInfo(String s) {
		System.out.println("\n--- " + s + " ---");
	}
	
	public void print(List<StorageObject> objs) {
		if (objs == null || objs.size() == 0) {
			System.out.println("\t No object was found.");
		} else {
			for (StorageObject obj: objs) {
				System.out.println("\t" + obj.toString());
			}
		}
	}
	
	public void disconnect() {
		_swiftClient.disconnect();
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		org.apache.log4j.Logger log4j = org.apache.log4j.LogManager.getLogger("org.apache.http.wire");
		log4j.setLevel(org.apache.log4j.Level.OFF);
		FolderStructure fs = new FolderStructure();
		fs.createFiles();
		//fs.list();
		//fs.delete();
		//fs.listMeta("a/fa1.txt");
		fs.expire("a/fa1.txt");
		fs.disconnect();
	}
}
