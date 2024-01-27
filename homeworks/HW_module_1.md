# HomeWork #1: Module 1 - Introduction & Prerequisites

## Docker & SQL

### Question 1. Knowing docker tags
    Run the command to get information on Docker
    docker --help
    Now run the command to get help on the "docker build" command:
    docker build --help
    Do the same for "docker run".
    Which tag has the following text? - Automatically remove the container when it exits
    
    Answer:   
        docker run --help 
            --rm    Automatically remove the container when it exits

### Question 2. Understanding docker first run
    Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).
    What is version of the package wheel?

    Answer:
        wheel      0.42.0

## Prepare Postgres

### Question 3. Count records
    How many taxi trips were totally made on September 18th 2019?
    Tip: started and finished on 2019-09-18.
    Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

    SQL query:
        ``` 
        SELECT COUNT(*) FROM green_taxi_trips
            WHERE lpep_pickup_datetime::date = '2019-09-18';
        ```
    Answer:
        15767

### Question 4. Largest trip for each day
    Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
    
    SQL query:
        SELECT "lpep_pickup_datetime" FROM "green_taxi_trips" WHERE "trip_distance" = (
	        SELECT MAX("trip_distance") FROM "green_taxi_trips"	
        );

    Answer:
        2019-09-26

### Question 5. Three biggest pick up Boroughs
    Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
    Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
   
    SQL query:
        SELECT def."Borough", SUM(trips."total_amount") AS total_amount
        FROM "taxi_zone_def" def 
        JOIN "green_taxi_trips" trips ON def."LocationID" = trips."PULocationID"
        WHERE def."Borough" != 'Unknown' 
            AND trips."lpep_pickup_datetime"::date = '2019-09-18'
        GROUP BY def."Borough"
        ORDER BY total_amount DESC;
    
    Answer:
        "Brooklyn" "Manhattan" "Queens"

### Question 6. Largest tip
    For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
    Note: it's not a typo, it's tip , not trip

    SQL query:
        SELECT "Zone" FROM "taxi_zone_def" WHERE "LocationID" = (
            SELECT "DOLocationID"
            FROM "green_taxi_trips"
            WHERE "tip_amount" = (
                SELECT MAX("tip_amount")
                FROM "green_taxi_trips"
                WHERE "PULocationID" = (
                    SELECT "LocationID"
                    FROM "taxi_zone_def"
                    WHERE "Zone" = 'Astoria'
                )
            )
        );
    
    Answer:
        JFK Airport

## Terraform

### Question 7. Creating Resources
    After updating the main.tf and variable.tf files run:
    `terraform apply`
    Paste the output of this command into the homework submission form.

    Answer:
    ```
    (base) ramazan@de-zoomcamp:~/my_zoomcamp/module_1/terraform_GCP$ terraform apply
    var.project
    Name of the project

    Enter a value: fresh-ocean-412204


    Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
    + create

    Terraform will perform the following actions:

    # google_bigquery_dataset.demo_dataset will be created
    + resource "google_bigquery_dataset" "demo_dataset" {
        + creation_time              = (known after apply)
        + dataset_id                 = "demo_dataset"
        + default_collation          = (known after apply)
        + delete_contents_on_destroy = false
        + effective_labels           = (known after apply)
        + etag                       = (known after apply)
        + id                         = (known after apply)
        + is_case_insensitive        = (known after apply)
        + last_modified_time         = (known after apply)
        + location                   = "US"
        + max_time_travel_hours      = (known after apply)
        + project                    = "fresh-ocean-412204"
        + self_link                  = (known after apply)
        + storage_billing_model      = (known after apply)
        + terraform_labels           = (known after apply)
        }

    # google_storage_bucket.demo-bucket will be created
    + resource "google_storage_bucket" "demo-bucket" {
        + effective_labels            = (known after apply)
        + force_destroy               = true
        + id                          = (known after apply)
        + location                    = "US"
        + name                        = "fresh-ocean-412204"
        + project                     = (known after apply)
        + public_access_prevention    = (known after apply)
        + rpo                         = (known after apply)
        + self_link                   = (known after apply)
        + storage_class               = "STANDARD"
        + terraform_labels            = (known after apply)
        + uniform_bucket_level_access = (known after apply)
        + url                         = (known after apply)

        + lifecycle_rule {
            + action {
                + type = "AbortIncompleteMultipartUpload"
                }
            + condition {
                + age                   = 1
                + matches_prefix        = []
                + matches_storage_class = []
                + matches_suffix        = []
                + with_state            = (known after apply)
                }
            }
        }

    Plan: 2 to add, 0 to change, 0 to destroy.

    Do you want to perform these actions?
    Terraform will perform the actions described above.
    Only 'yes' will be accepted to approve.

    Enter a value: yes

    google_bigquery_dataset.demo_dataset: Creating...
    google_storage_bucket.demo-bucket: Creating...
    google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/fresh-ocean-412204/datasets/demo_dataset]
    google_storage_bucket.demo-bucket: Creation complete after 1s [id=fresh-ocean-412204]

    Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
    ```
    