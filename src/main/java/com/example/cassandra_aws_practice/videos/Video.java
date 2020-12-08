package com.example.cassandra_aws_practice.videos;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
@AllArgsConstructor
public class Video {

    private UUID id;
    private String title;
    private Instant creationDate;
}
