# Scaling Discussion
This code has made to be a building block to be a scale ingestion program. 

* Inside a single machine, the program is utilising spark process with optimised parquet file system. This is done to increase the efficiency of data ingestion and aggregation. The parquet file system is chosen as it provides efficient columnar storage, which is advantageous for the aggregation process. 

* This program will also utilise multiple core inside the local machine by default, as I am using `local[*]` argument when initializing spark context.

* The program is designed to be scaled horizontally by utilizing a distributed computing framework which is Spark.

Now how to scale this framework to be able to handle multiple ingestion job at once? As this meant to be a simplified version of review ingestion framework as stated in design document, most of the point has been discussed there. But to put it into a consise bullet point:

* Use cloud provider so it can be scaled easily across multiple cloud service and computing, this mean we need 3 core component:
  * Blob Storage (S3), to store input and output file system acting as datalake (instead of local file system)
  * Job orchestration/scheduler service (Airflow), this will orchestrating and scheduling the ingestion job.
  * On-demand spark based job processing service (Databrick, AWS Glue, EMR, or simple multiple EC2 as spark cluster).

* The program has been designed to make it simple to create a new job by:
  * Simply create new config file, so new job can be easily created without the need to write new python/pyspark code.
  * Orchestration service simply will run the job with different parameter.
  * Orchestration service can delegate the heavy processing task to the On-demand job transformation service as stated above. 
  * If needed, orchestration service (in this case, Airflow) can be scaled horizontally into multiple worker pod.

* To demonstrate this system, I made a `docker-compose.yml` and `Dockerfile` that can be use to build spark cluster master-worker by running it in multiple EC2 instance. Then the job can utilise multiple cluster spark job instead of single worker local spark. Though, this will introduce too much operational overhead given we need to manage the EC2 orchestration also. I would personally prefer a managed service for this.

* To summarise, every component here are designed to be horizontally scaled, all in term of processing resource, number of job (by using on-demand job), and orchestration. Then storage blob (S3) will be used as shared resource to store input and output.

## Additional Operational Optimization
* Additionally, delta format is used to store the output as it support MERGE and UPDATE operations without the need of actual traditional datawarehouse database.

* To make it more scale and performant, regular optimization  of delta storage can also be done as [recommended by Delta](https://docs.delta.io/latest/best-practices.html) (can be scheduled by another Airflow job).

* Job creation from config file can be integrated into CI/CD pipeline, so all the user need to do is commit new config file inside `ingestion_config/` and new job will be orchestrated automatically.
