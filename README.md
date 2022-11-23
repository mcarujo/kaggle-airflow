# airflow-examples
Airflow scripts + Docker (For Academic reason)
```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker-compose up -d
```
