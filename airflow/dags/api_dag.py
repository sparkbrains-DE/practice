import logging
from datetime import datetime
import requests
from airflow.decorators import dag, task
from typing import Dict

from airflow.operators.email import EmailOperator




API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"



@dag(
	"api_extraction_dag",
	schedule="@daily",
	catchup=False

	)


def taskflow():

	@task(
		task_id="extract_from_api",
		retries=1
		)

	def extraction_task():
		return requests.get(API).json()["bitcoin"]




	@task(
		multiple_outputs=True
		)
	def transformation_task(response):
		logging.info(response)
		return {"usd": response["usd"], "change": response["usd_24h_change"]}





	@task
	def store_task(data):
		logging.info(f"Store: {data['usd']} with change {data['change']}")




	email_notify = EmailOperator(
		task_id = "email_notify",
		to = "hashtagsukh@gmail.com",
		subject = "Airflow dag completed",
		html_content = "----------- Airflow dag completed body ------------"

		)



	store_task(transformation_task(extraction_task())) >> email_notify




taskflow()