package com.sharingapples.test.sync.store.jdbc.setup;

import com.sharingapples.sync.resource.Many;
import com.sharingapples.sync.resource.ResourceMarker;
import com.sharingapples.sync.resource.annotations.Field;

/**
 * Created by ranjan on 12/13/15.
 */
public class Book implements ResourceMarker {

  @Field private Long id;
  @Field private String title;
  @Field private String isbn;
  @Field private Publisher publisher;
  @Field private Many<Author> authors;


  @Override
  public Long getId() {
    return id;
  }

  public String getTitle() { return title; }

  public String getISBN() { return isbn; }

  public Publisher getPublisher() { return publisher; }

  public Many<Author> getAuthors() { return authors; }

}
