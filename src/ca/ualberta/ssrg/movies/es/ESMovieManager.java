package ca.ualberta.ssrg.movies.es;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import android.util.Log;
import ca.ualberta.ssrg.movies.es.data.Hits;
import ca.ualberta.ssrg.movies.es.data.SearchHit;
import ca.ualberta.ssrg.movies.es.data.SearchResponse;
import ca.ualberta.ssrg.movies.es.data.SimpleSearchCommand;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
//restful: cant do transsctions, wheatever we ask the server it will get put in. 
//json, document store, in storage system, does not enforce consistency, only stores a collection of blobs, URL: server/index/type/id, store things in ellastic search
//use POST to create a document
//use GET gets a document from the server
//use GET with curl, curl is a command that takes a url, its cat url, put whaterve is on 
//XGET gives you the default get

// curl -XPOST 'link' -d'json document-d' ; echo
//elastisearch will always reurn some kind of json
//use PUT o overwrite a document 
//use DELETE to delete a document
//searching in elastic search, use the post command for search 
public class ESMovieManager {

	private static final String TAG = "MovieSearch";
	private Gson gson;
	private Movies movies = new Movies();

	public Movies getMovies() {
		return movies;
	}

	public ESMovieManager(String search) {
		gson = new Gson();
		searchMovies(search, null);
	}

	/**
	 * Get a movie with the specified id
	 */
	public Movie getMovie(int id) {//just like GET with curl
		SearchHit<Movie> sr = null;
		HttpClient httpClient = new DefaultHttpClient();//http client works like curl, built in android
		HttpGet httpGet = new HttpGet(movies.getResourceUrl() + id);//construct url with server/index/type/id

		HttpResponse response = null;

		try {
			response = httpClient.execute(httpGet);//giv command to httclient to execute
		} catch (ClientProtocolException e1) {//correctway to handle exceptions, default= e.printStackTrace(), dont do it, makes it hard to debug application, fact doesnt show until way down the line
			throw new RuntimeException(e1);//instead of finding the failing stuff, it keeps going^
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		
		Type searchHitType = new TypeToken<SearchHit<Movie>>() {}.getType();

		try {
			sr = gson.fromJson(
					new InputStreamReader(response.getEntity().getContent()),
					searchHitType);
		} catch (JsonIOException e) {
			throw new RuntimeException(e);
		} catch (JsonSyntaxException e) {
			throw new RuntimeException(e);
		} catch (IllegalStateException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return sr.getSource();

	}

	/**
	 * Get movies with the specified search string. If the search does not
	 * specify fields, it searches on all the fields.
	 */
	public void searchMovies(String searchString, String field) {
		Movies result = new Movies();

		/**
		 * Creates a search request from a search string and a field
		 */

		HttpPost searchRequest = new HttpPost(movies.getSearchUrl());

		String[] fields = null;
		if (field != null) {
			throw new UnsupportedOperationException("Not implemented!");
		}

		SimpleSearchCommand command = new SimpleSearchCommand(searchString);

		String query = gson.toJson(command);
		Log.i(TAG, "Json command: " + query);

		StringEntity stringEntity = null;
		try {
			stringEntity = new StringEntity(query);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}

		searchRequest.setHeader("Accept", "application/json");
		searchRequest.setEntity(stringEntity);
		
		HttpClient httpClient = new DefaultHttpClient();
		
		HttpResponse response = null;
		try {
			response = httpClient.execute(searchRequest);
		} catch (ClientProtocolException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		/**
		 * Parses the response of a search
		 */
		Type searchResponseType = new TypeToken<SearchResponse<Movie>>() {
		}.getType();
		
		try {
			SearchResponse<Movie> esResponse = gson.fromJson(
					new InputStreamReader(response.getEntity().getContent()),
					searchResponseType);
		} catch (JsonIOException e) {
			throw new RuntimeException(e);
		} catch (JsonSyntaxException e) {
			throw new RuntimeException(e);
		} catch (IllegalStateException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Extract the movies from the esResponse and put them in result

		movies.notifyObservers();
	}
}
