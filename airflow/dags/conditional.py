from airflow.decorators import dag, task
import random
import pendulum


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False
)
def conditionals():

    @task
    def choose_number():
        num = random.choice([1, 2])
        return num   # Return automatically pushes XCom

    @task.branch
    def decider_task(num):
        if num == 1:
            return "task_1"
        else:
            return "task_2"

    @task
    def task_1():
        return "this is task 1 output"

    @task
    def task_2():
        return "this is task 2 output"

    number = choose_number()
    branch = decider_task(number)

    number >> branch >> [task_1(), task_2()]


conditionals()
