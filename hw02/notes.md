## HeadingHomework 02
Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/01-docker-terraform/homework.md

I used [docker-compose.yml](./docker-compose.yml) to run Kestra.

To finish the task, I've [slightly updated the example flow](./flow.yml) to be able to backfill the whole data at once for each taxi type.


### Question 1
Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?


Answer:

Outputs → extract → outputFiles → yellow_tripdata_2020-12.csv

    128.3 MB

 
### Question 2

What is the rendered value of the variable  `file`  when the inputs  `taxi`  is set to  `green`,  `year`  is set to  `2020`, and  `month`  is set to  `04`  during execution?

Answer:

    green_tripdata_2020-04.csv

  
### Question 3
How many rows are there for the  `Yellow`  Taxi data for all CSV files in the year 2020?

SQL:

    SELECT COUNT(*) FROM public.yellow_tripdata
    WHERE filename LIKE 'yellow_tripdata_2020-__.csv';

Answer:

    24648499

### Question 4 
How many rows are there for the  `Green`  Taxi data for all CSV files in the year 2020?

SQL:

    SELECT COUNT(*) FROM public.green_tripdata
    WHERE filename LIKE 'green_tripdata_2020-__.csv';

Answer:

    1734051


### Question 5
How many rows are there for the  `Yellow`  Taxi data for the March 2021 CSV file?

SQL:

    SELECT COUNT(*) FROM public.yellow_tripdata
    WHERE filename = 'yellow_tripdata_2021-03.csv';

Answer:

    1925152

### Question 6
How would you configure the timezone to New York in a Schedule trigger?

Answer: https://kestra.io/docs/workflow-components/triggers/schedule-trigger