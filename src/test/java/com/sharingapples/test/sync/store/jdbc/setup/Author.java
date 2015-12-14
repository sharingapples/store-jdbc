package com.sharingapples.test.sync.store.jdbc.setup;

import com.sharingapples.sync.resource.ResourceMarker;
import com.sharingapples.sync.resource.annotations.Field;

/**
 * Created by ranjan on 12/13/15.
 */
public class Author implements ResourceMarker {
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
