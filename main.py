from src.demo_landing_to_raw import landing_to_raw
from yetl.flow import Timeslice
import json

# timeslice = Timeslice(2021, 1, 1)
timeslice = Timeslice(2021, 1, 2)

table = "customer_details"
results = landing_to_raw(table=table, timeslice=timeslice)
results = json.dumps(results, indent=4, default=str)
print(results)

table = "customer_preferences"
results = landing_to_raw(table=table, timeslice=timeslice)
results = json.dumps(results, indent=4, default=str)
print(results)

