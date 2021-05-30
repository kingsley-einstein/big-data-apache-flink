package com.spring.apache.flink.model;

public class Movie implements java.io.Serializable {

  private Long serialNumber;
  private String title;
  private String genre;

  public Movie() {}

  public Movie(
    final Long serialNumber,
    final String title,
    final String genre
  ) {
    this.serialNumber = serialNumber;
    this.title = title;
    this.genre = genre;
  }

  public void setSerialNumber(final Long serialNumber) {
    this.serialNumber = serialNumber;
  }

  public Long getSerialNumber() {
    return serialNumber;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

  public String getTitle() {
    return title;
  }

  public void setGenre(final String genre) {
    this.genre = genre;
  }

  public String getGenre() {
    return genre;
  }
}
