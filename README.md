# healthcare-mrf-api
US Healthcare Machine Readable Files API with Importer

Docker Dev builds can be pulled from:
```shell
docker pull dmytronikolayev/healthcare-mrf-api
```

Please use .env.example file for the configuration reference.
You will need to have PostgreSQL and Redis to run this project.
To use API you will need to execute the import for the first time after the container start.

Crontab example to run weekly updates of the data from FDA API:
```shell
15 5 * * 1 docker exec CHANGE_TO_THE_NAME_OF_DRUG_API_CONTAINER /bin/bash -c 'source venv/bin/activate && python main.py start mrf && python main.py worker process.MRF --burst' > /dev/null 2>&1
```

Once you do the first import(the comand that you added to crontab) - the data will be available on 8080 port of your Docker image.


## Examples

The status of your import will be seen here: ``http(s)://YourHost:8080/api/v1/import``

Some plan info (Put your Plan ID): ``http://127.0.0.1:8085/api/v1/plan/id/99999GG1234005``

## Additional Info

Please check [Healthcare Machine Readable Files Import Data](https://pharmacy-near-me.com/healthcare-data/) Status Page. It shows the work of the Importer with status of Data from Healthcare Issuers (Insurance companies/agents).

Project by [Pharmacy Near Me](https://pharmacy-near-me.com/)

For testing purposes the API works and getting heavily tested in [Drugs Discount Card](https://pharmacy-near-me.com/drug-discount-card/)


