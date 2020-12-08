package com.example.cassandra_aws_practice.videos;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class VideoController {

    private VideoRepository videoRepository;

    @Autowired
    public VideoController(VideoRepository videoRepository) {
        this.videoRepository = videoRepository;
    }

    @PostMapping("/videos/default-video")
    public ResponseEntity<Video> insertDefaultVideo(){
        Video video = new Video(null, "default-title", Instant.now());
        return new ResponseEntity(videoRepository.insertVideo(video), HttpStatus.CREATED);
    }

    @PostMapping("/videos")
    public ResponseEntity<Video> insertVideo(@RequestBody Video video){
        return new ResponseEntity(videoRepository.insertVideo(video), HttpStatus.CREATED);
    }

    @GetMapping("/videos")
    public ResponseEntity<List<Video>> selectAll(){
        return new ResponseEntity(videoRepository.selectAll(), HttpStatus.OK);
    }

    @GetMapping("/videos/{id}")
    public ResponseEntity<Video> selectById(@PathVariable("id") UUID videoId){
        return new ResponseEntity(videoRepository.selectById(videoId), HttpStatus.OK);
    }

    @DeleteMapping("/videos")
    public ResponseEntity deleteAll(){
        videoRepository.deleteAll();
        return new ResponseEntity(null, HttpStatus.OK);
    }

    @DeleteMapping("/videos/{id}")
    public ResponseEntity deleteById(@PathVariable("id") UUID videoId){
        videoRepository.deleteById(videoId);
        return new ResponseEntity(null, HttpStatus.OK);
    }

    @DeleteMapping("/videos/table")
    public ResponseEntity dropTable(){
        videoRepository.dropVideoTable();
        return new ResponseEntity(null, HttpStatus.OK);
    }

    @DeleteMapping("/videos/keyspace")
    public ResponseEntity dropKeyspace(){
        videoRepository.dropKeyspace();
        return new ResponseEntity(null, HttpStatus.OK);
    }
}
