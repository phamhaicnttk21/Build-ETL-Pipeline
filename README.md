![img.png](img.png)


***NOTE*** WSL
Must implement 
docker exec -it airflow_webserver airflow db init   (must implement airflow init)


** Xử lí khi login fail -> do user không tồn tại **
docker exec -it airflow_webserver airflow users create \
    --username admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin



