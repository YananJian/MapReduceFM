import json
import subprocess
import sys

def start_jobtracker(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("mr.core.JobTrackerImpl")
  cmd.append(config['mr_registry']['host'])
  cmd.append(config['mr_registry']['port'])
  cmd.append(config['dfs_registry']['port'])
  cmd.append(config['jobtracker port'])
  cmd.append(config['reducer'])
  subprocess.call(cmd)

if __name__ == "__main__":
  start_jobtracker(sys.argv)
