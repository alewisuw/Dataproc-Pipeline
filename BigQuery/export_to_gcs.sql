EXPORT DATA  
  OPTIONS ( 
    uri = 'gs://dataeng_stackoverflow_data/data/*.parquet', 
    format = 'PARQUET', 
    compression = 'SNAPPY',
    overwrite = true)
AS (
  SELECT id, creation_date, title, body, tags, view_count FROM `bigquery-public-data.stackoverflow.posts_questions`
);