package com.sharingapples.test.sync.store.jdbc.setup;

import com.sharingapples.sync.resource.Resource;
import com.sharingapples.sync.resource.annotations.Field;
import com.sharingapples.sync.resource.annotations.ResourceMarker;

/**
 * Created by ranjan on 12/13/15.
 */
@ResourceMarker(name=Author.NAME)
public class Author implements Resource {
  public static final String NAME = "author";

  @Field private Long id;
  @Field private String name;

  @Override
  public Long getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
