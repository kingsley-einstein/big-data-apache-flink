package com.spring.apache.flink.controller;

import com.spring.apache.flink.model.Movie;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.stream.Collectors;
//import com.spring.apache.flink.model.Movie;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1")
public class DataController {

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
}
