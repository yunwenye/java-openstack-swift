package ye.yunwen.swift.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import ch.iterate.openstack.swift.Client;
import ch.iterate.openstack.swift.method.Authentication10UsernameKeyRequest;
import ch.iterate.openstack.swift.method.AuthenticationRequest;
import ch.iterate.openstack.swift.AuthenticationResponse;

public class SwiftAccesser {

	public static void testAccess() throws URISyntaxException, IOException {
		HttpClient httpclient = new DefaultHttpClient();
		Client client = new Client(httpclient);
		
		URI uri = new URI("http://54.226.238.78:8080/auth/v1.0");
		AuthenticationRequest authreq = new Authentication10UsernameKeyRequest(uri, "test:tester", "testing");
		AuthenticationResponse rsp = client.authenticate(authreq);
		
		System.out.println("[authToken=" + rsp.getAuthToken() + ", regions=" + rsp.getRegions());
	}
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		testAccess();
	}
}
