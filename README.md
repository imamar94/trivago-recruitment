# Case Study

Below are two tasks for you to work on. Please read through everything carefully before you start.
This case study is part of a zip file containing the following files: 

* [README.md](README.md)<br>
  Containing these case study instructions.
* [design_document.md](design_document.md)<br>
  A blank markdown file for you to provide the solution to task 1 in.
* [scaling_discussion.md](scaling_discussion.md)<br>
  A blank markdown file to provide the scaling discussion of task 2 in.
* [data/inappropriate_words.txt](data/inappropriate_words.txt)<br>
  An example file with inappropriate words to use for testing purposes
* [data/reviews.jsonl](data/reviews.jsonl)<br>
  A JSONL file containing example reviews for testing purposes
* [schemas/review.json](schemas/review.json)<br>
  A single document JSON file containing the JSON schema for the review.
* [schemas/aggregation.json](schemas/aggregation.json)<br>
  A single document JSON file containing the JSON schema for the aggregated review data.
* [Makefile](Makefile)<br>
  A makefile containing one or more blank targets that you can implement according to the needs of your implementation for task 2.

If you find you didn't receive these files, or there is something missing, please reach out, so we can clarify and fix the issue.
In addition to these files you are free to add any files and directories you need to fulfill both tasks.

## Task 1: Design A Review Ingestion System

### Scenario

You are working for a company that specializes in restaurant reviews and you are tasked with designing the new Review Ingestion System. Provide a design document proposing a solution that fulfills the following requirements.

### Requirements

The reviews we receive might contain inappropriate words, so we need to make sure that we filter these words out and replace them with asterisks (“****”). If a review contains too many inappropriate words in proportion to the overall text, we consider the entire review as inappropriate and need to filter it out. Furthermore, we are not interested in outdated reviews, as they might no longer reflect the reality of the restaurant today.

The Product department requests that we deliver the following aggregated metrics:

* Number of reviews
* Average score
* Average length of review
* Review age (in days)
  * Oldest
  * Newest
  * Average

### Constraints

Design your solution under the following assumptions and constraints

* The reviews are provided as JSONL (new-line delimited JSON) files uploaded into a BLOB storage system (S3, GCS, Azure Blob Storage, HDFS, CephFS, etc.)
  * You are free to choose which BLOB storage you want to use in your design.
  * Assume the reviews land in a dedicated bucket/namespace in the BLOB storage system and are prefixed by a date prefix.
  * All reviews are in English
* A review is considered outdated if it is more than 3 years old from the date of processing.
  * The threshold for "inappropriate words per review" until it is considered inappropriate as a whole is 20% of words of the entire text.
  * The reviews are supposed to adhere to the [included Review JSON schema](schemas/review.json).
* You are not in control of the upstream producer of these reviews, so consider the following:
  * Frequency of arrivals of new files in the storage
  * Maximum file size or number of lines in the files (there is at least one line per file)
  * You may receive duplicates
  * Completeness. The incoming file can't be considered a full import.
  * The incoming reviews might not adhere to the specified JSON schema.
  * Reviews may be updated over time.
* The list of inappropriate words is provided in the same BLOB storage system in a dedicated bucket/namespace as a one-word-per-line UTF-8 encoded txt file.
  * The file is updated independently of your solution's deployment.
  * The file is always guaranteed to be complete and valid.
* You are free to choose your deployment target
  * Public cloud environment like (AWS, Azure or GCP)
  * Private cloud/servers
    * Assuming necessary services are available, please focus on the Data Engineering aspects of your solution rather than the operational aspects.
* Your solution needs to be scalable.
* The aggregations should be written to a Data Warehouse / Data Lakehouse of your choice.
* You are free to choose between a streaming or a batch solution. The minimum frequency is once a day.
* The processed reviews must be written to a dedicated bucket/namespace of your BLOB storage system using a date(time) prefix.
* Discarded reviews should also be written to another dedicated bucket/namespace of your BLOB storage system.

### Design Document

Make sure your design document contains at least the following:

1. An architecture diagram of your proposed solution.
2. A diagram showing the flow of data through your solution.
3. A rough outline of your deployment and scaling strategies.

## Task 2: Implement the Processing Logic

Keeping the above scenario in mind, you are now tasked with implementing a simplified version of the processing logic as a locally runnable (executable binary, script, docker, etc.) application in a suitable language of your choice. In this version you don’t need to store discarded reviews.

The other constraints still apply such as the maximum age of a review and maximum percentage of inappropriate words until a reviews is considered unsuitable.
Additionally, the input file may be too large to completely fit into memory and the records may still not adhere to the schema provided.

Your application must accept the following CLI parameters:

* `--input`<br>
    Local filesystem path to the JSONL file containing the reviews
* `--inappropriate_words`<br>
    Local file         system path to the new-line delimited text file containing the words to filter out
* `--output`<br>
    Local filesystem path to write the successfully processed reviews to
* `--aggregations`<br>
    Local file system path to write the aggregations as JSONL

These are mandatory CLI parameters and will be used to test your solution.

The aggregations must adhere to the [included Aggregations JSON schema](schemas/aggregation.json).

While this task is only supposed to work on a single machine, make sure it can make use of a modern machine with multiple CPU cores, if your language of choice allows it.

Also, briefly discuss how you would scale the code you have written across multiple machines if the need were to arise. Don’t mistake this for a requirement to implement a solution that can be run in a distributed way, just discuss the possibility.
Remember to write your answer in the provided [scaling_discussion.md](scaling_discussion.md) document.