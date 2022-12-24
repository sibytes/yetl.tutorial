from src.demo_landing_to_raw import landing_to_raw
from yetl.flow import Timeslice, OverwriteSave
from yetl.workflow import multithreaded as yetl_wf
import yaml

# timeslice = Timeslice(year=2021, month=1, day=1)
# timeslice = Timeslice(year=2021, month=1, day=2)
timeslice = Timeslice(year="*", month="*", day="*")
project = "demo"
maxparallel = 2

path = f"./config/project/{project}/{project}_tables.yml"

with open(path, "r", encoding="utf-8") as f:
    metdata = yaml.safe_load(f)

tables: list = [t["table"] for t in metdata.get("tables")]

yetl_wf.load(project, tables, landing_to_raw, timeslice, OverwriteSave, maxparallel)
