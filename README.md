# jira_postgres
How to query informations out of Jira in a postgreSQL database.
With airflow you can schedule it automaticaly, instead of airflow you can also trigger the code manually. Therefore just skip the airflow parts and set up the used connections in th code itself. 

## Prerequisities:
### Airflow
Set up the connections in your (local) airflow.
How-to: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html 

Be sure your used Jira Connection (the User) is a member of the jira project. 

### PostgreSQL
Accesible postgres database with columns matching the queried information out of th jira project. 

