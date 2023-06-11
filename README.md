# Data Pipelines


## Starting Airflow + Dags

Using the Airflow documentation s reference, it's only necessary to run the following commands and make some environmental adjustments regarding the access keys and other secrets. 😁

```bash
mkdir -p ./dags ./logs ./plugins 
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker-compose up -d
```

## The goal
Extract data from different sources, transform them and load them at Kaggle plataform (ETL).
![alt text](https://github.com/mcarujo/airflow-examples/raw/main/datapipeline.png)


### Datasets coverd by the repository:
- [portugal-proprieties-rent-buy-and-vacation](https://www.kaggle.com/datasets/mcarujo/portugal-proprieties-rent-buy-and-vacation)
- [european-football-season-202223](https://www.kaggle.com/datasets/mcarujo/european-football-season-202223) [finished]
- [south-america-football-season-2023](https://www.kaggle.com/datasets/mcarujo/south-america-football-season-2023)
