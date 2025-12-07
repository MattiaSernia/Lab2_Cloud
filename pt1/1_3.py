import time
from locust import HttpUser, task, between

class QuickstartUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def integration_test(self):
        self.client.get("/numericalintegralservice/0.0/3.14159")