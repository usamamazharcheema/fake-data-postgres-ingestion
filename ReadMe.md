I have built a small pipeline that handles the insertion of fake data into both a CSV file and a PostgreSQL table using Apache Airflow.

To run this pipeline, you can use the following command: docker-compose up. Once it's up and running, you can inspect the output through either of the following methods:

1. Go to the the data folder and open the output.csv file.
2. Open this link: http://localhost:8080/. When prompted, enter the username 'airflow' and the password 'airflow.' Next, click on the 'functions_insertion' DAG. In the Graph menu, press the play button to trigger the DAG. Then, click on the nodes in the graph and navigate to the log section to view the corresponding outputs.