import json
import subprocess
import sys

def dfs(argv):
  if argv[1] == "describe":
    describe_dfs()
  elif argv[1] == "put":
    put_file(argv)
  elif argv[1] == "get":
    get_file(argv)

def describe_dfs():
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.DFSDescriber")
  cmd.append(config['registry']['host'])
  cmd.append(config['registry']['port'])
  subprocess.call(cmd)

def put_file(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.FileUploader")
  cmd.append(argv[2])
  cmd.append(argv[3])
  cmd.append(argv[4])
  cmd.append(config['registry']['host'])
  cmd.append(config['registry']['port'])
  subprocess.call(cmd)

def gett_file(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.FileDownloader")
  cmd.append(argv[2])
  cmd.append(argv[3])
  cmd.append(config['registry']['host'])
  cmd.append(config['registry']['port'])
  subprocess.call(cmd)

if __name__ == "__main__":
  dfs(sys.argv)
