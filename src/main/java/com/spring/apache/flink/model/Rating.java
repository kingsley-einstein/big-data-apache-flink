package com.spring.apache.flink.model;

public class Rating implements java.io.Serializable {

  private Long userId;
  private Long movieId;
  private Double rating;
  private Long timestamp;

  public Rating() {}

  public Rating(
    final Long userId,
    final Long movieId,
    final Double rating,
    final Long timestamp
  ) {
    this.userId = userId;
    this.movieId = movieId;
    this.rating = rating;
    this.timestamp = timestamp;
  }

  public Long getUserId() {
    return userId;
  }

  public Long getMovieId() {
    return movieId;
  }

  public Double getRating() {
    return rating;
  }

  public Long getTimestamp() {
    return timestamp;
  }
}
