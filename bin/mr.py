import json
import subprocess
import sys

def mr(argv):
  if len(argv) == 2:
    if argv[1] == "describe":
      describe_jobs()
    elif argv[1] == "terminate":
      terminate()
  if len(argv) > 2:
      if (argv[1] == 'describe'):
          describe_job(argv[2])

def describe_jobs():
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("mr.MRDescriber")
  cmd.append(config['mr_registry']['host'])
  cmd.append(config['mr_registry']['port'])
  subprocess.call(cmd)

def describe_job(jobID):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("mr.MRDescriber")
  cmd.append(config['mr_registry']['host'])
  cmd.append(config['mr_registry']['port'])
  cmd.append(jobID)
  subprocess.call(cmd)

def terminate():
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("mr.MRTerminator")
  cmd.append(config['mr_registry']['host'])
  cmd.append(config['mr_registry']['port'])
  subprocess.call(cmd)

if __name__ == "__main__":
  mr(sys.argv)
