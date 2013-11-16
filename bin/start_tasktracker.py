import json
import subprocess
import sys

def start_tasktracker(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("mr.core.TaskTrackerImpl")
  cmd.append(config['mr_registry']['host'])
  cmd.append(config['mr_registry']['port'])
  cmd.append(config['dfs_registry']['port'])
  cmd.append(config['tasktracker'][int(argv[1])]['port'])
  cmd.append(argv[1])
  cmd.append(config['tasktracker'][int(argv[1])]['dir'])
  cmd.append(config['reducer'])
  subprocess.call(cmd)

if __name__ == "__main__":
  start_tasktracker(sys.argv)
