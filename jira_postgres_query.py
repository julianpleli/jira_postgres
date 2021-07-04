# This DAG writes the informations out of a Jira  SD project in the postgres database
import os
import time
import logging
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# Set up the Connection in Airflow before
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Set up the Connection in Airflow before
from airflow.providers.jira.hooks.jira import JiraHook

log = logging.getLogger(__name__)


def query_data(**kwargs):

    atc_conn = JiraHook("atc_sd_jira").get_conn()

    try:
        postgres_conn = postgres_hook.get_conn()
        cursor = postgres_conn.cursor()
        logging.info("Connected to PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        logging.info(error)

    # Query the relevant issues, be sure to have access to the jira-project
    jql = "project in <project_name>\
            AND createdDate >= \"2021/01/01\""

    issues = atc_conn.search_issues(jql,
                                    startAt=0,
                                    maxResults=0)  # Default of the search is 50 tickets, set it to 0 to recieve all results

    logging.info('Found Tickets for JQL: ' + str(issues.total))

    # Recieve the relevant information issue for issue
    for issue in issues:
        Ticket_Key = issue.key
        Ticket_Creation = issue.fields.created
        Ticket_Project = issue.fields.project.key

        # SQL to insert new tickets and update the existing ones
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jira.reporting
            (ticketkey, createdat, project)
            VALUES
                (%s, %s, %s)
            ON CONFLICT(ticketkey) DO UPDATE
            SET
                createdat=excluded.createdat,
                project=excluded.project;
                   """, (Ticket_Key, Ticket_Creation, Ticket_Project)
                       )

    # Cleanup
    conn.commit()
    cursor.close()
    conn.close()


# Airflow
DEFAULT_ARGS = {
    'owner': '<Owner>',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 0,
    'on_failure_callback': on_failure,
    'on_success_callback': on_failure
}

JIRA_DATA_DAG = DAG(
    dag_id='query_data',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

atc_query_data = PythonOperator(task_id="query_data",
                                provide_context=True,
                                python_callable=query_data,
                                dag=JIRA_DATA_DAG)


# Kontrollstruktur
if __name__ == '__main__':
    query_data()
