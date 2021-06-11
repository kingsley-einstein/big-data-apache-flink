package com.spring.apache.flink.controller;

import com.spring.apache.flink.model.Movie;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1")
public class DataController {

  private static class IdKeySelectorMovie
    implements KeySelector<Tuple3<Long, String, String>, Long> {

    @Override
    public Long getKey(Tuple3<Long, String, String> value) {
      return value.f0;
    }
  }

  private static class IdKeySelectorRating
    implements KeySelector<Tuple4<Long, Long, Double, Long>, Long> {

    @Override
    public Long getKey(Tuple4<Long, Long, Double, Long> value) {
      return value.f1;
    }
  }

  @GetMapping("/listMovies")
  public List<Movie> getMovies() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Tuple3<Long, String, String>> movies = env
      .readCsvFile(
        FileSystems
          .getDefault()
          .getPath(
            "C://Coding-Projects/spring-big-data-apache-flink/ml-latest-small/movies.csv"
          )
          .toString()
      )
      .ignoreFirstLine()
      .parseQuotedStrings('"')
      .ignoreInvalidLines()
      .types(Long.class, String.class, String.class);

    return movies
      .collect()
      .stream()
      .map(t -> new Movie(t.f0, t.f1, t.f2))
      .collect(Collectors.toList());
  }

  @GetMapping("/movies_ratings")
  public List<Tuple2<Tuple3<Long, String, String>, Tuple4<Long, Long, Double, Long>>> getMoviesAndRatings()
    throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Tuple3<Long, String, String>> movies = env
      .readCsvFile(
        (
          FileSystems
            .getDefault()
            .getPath(
              "C://Coding-Projects/spring-big-data-apache-flink/ml-latest-small/movies.csv"
            )
            .toString()
        )
      )
      .ignoreFirstLine()
      .parseQuotedStrings('"')
      .ignoreInvalidLines()
      .types(Long.class, String.class, String.class);

    DataSet<Tuple4<Long, Long, Double, Long>> ratings = env
      .readCsvFile(
        FileSystems
          .getDefault()
          .getPath(
            "C://Coding-Projects/spring-big-data-apache-flink/ml-latest-small/ratings.csv"
          )
          .toString()
      )
      .ignoreFirstLine()
      .parseQuotedStrings('"')
      .ignoreInvalidLines()
      .types(Long.class, Long.class, Double.class, Long.class);

    return movies
      .join(ratings)
      .where(new IdKeySelectorMovie())
      .equalTo(new IdKeySelectorRating())
      .collect();
  }
}
