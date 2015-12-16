package com.sharingapples.test.sync.store.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sharingapples.sync.init.InitProcess;
import com.sharingapples.sync.resource.Registrar;
import com.sharingapples.sync.state.State;
import com.sharingapples.sync.store.Engine;
import com.sharingapples.sync.store.ResourceCache;
import com.sharingapples.sync.store.StorageSession;
import com.sharingapples.sync.store.jdbc.EngineJDBC;
import com.sharingapples.sync.store.jdbc.StoreSqlite;
import com.sharingapples.test.sync.store.jdbc.setup.Author;
import com.sharingapples.test.sync.store.jdbc.setup.Book;
import com.sharingapples.test.sync.store.jdbc.setup.Publisher;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by ranjan on 12/13/15.
 */
public class StoreTestCase {

  private Registrar registrar;

  @Before
  public void init() {
    registrar = new Registrar(source -> {
      source.registerResource(Book.class);
      source.registerResource(Author.class);
      source.registerResource(Publisher.class);
    });
  }

  @Test
  public void testStorage() throws IOException {
    String publisherJSON = "{\"id\":null, \"name\":\"Real Time Solutions\"}";
    String authorJSON = "{\"id\":null,\"name\":\"Ranjan Shrestha\"}";
    String bookJSON = "{" +
            "\"id\":null, " +
            "\"title\":\"Java and Javascript & React\", " +
            "\"ISBN\":null," +
            "\"publisher\":" + publisherJSON + ", " +
            "\"authors\":[" + authorJSON + "]" +
            "}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode publisherNode = mapper.readTree(publisherJSON);
    JsonNode authorNode = mapper.readTree(authorJSON);
    JsonNode bookNode = mapper.readTree(bookJSON);

    Book book = new Book();
    //book.setJSON((ObjectNode) bookNode);

    StoreSqlite store = new StoreSqlite(registrar, File.createTempFile("store-jdbc-", ".sqlite"));

    store.execute(engine -> {

      EngineJDBC e = (EngineJDBC) engine;
      e.createTable(Book.class);
      e.createTable(Author.class);
      e.createTable(Publisher.class);

      return null;
    });

    ResourceCache<Book> newBook = (ResourceCache<Book>)store.execute(engine -> {
      return engine.insert(book);
    });





//
//
//    State.getResourceMap(Book.class).remove(bk);
//
//
//    RecordSet<Book> books = store.fetchAll(State.getResourceMap(Book.class));
//    while(books.hasNext()) {
//      Book b = books.next();
//      System.out.println(b.getTitle());
//      System.out.println(b.getId());
//
//    }


  }

}
