import json
import subprocess
import sys

def dfs(argv):
  if argv[1] == "describe":
    describe_dfs()
  elif argv[1] == "put_text":
    put_textfile(argv)
  elif argv[1] == "get_text":
    get_textfile(argv)
  elif argv[1] == "put_class":
    put_clsfile(argv)
  elif argv[1] == "get_class":
    get_clsfile(argv)
  elif argv[1] == 'terminate':
    terminate()

def describe_dfs():
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.DFSDescriber")
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  subprocess.call(cmd)

def put_textfile(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.FileUploader")
  cmd.append(argv[2])
  cmd.append(argv[3])
  cmd.append(argv[4])
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  subprocess.call(cmd)

def get_textfile(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.FileDownloader")
  cmd.append(argv[2])
  cmd.append(argv[3])
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  subprocess.call(cmd)

def put_clsfile(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.ClassUploader")
  cmd.append(argv[2])
  cmd.append(argv[3])
  cmd.append(argv[4])
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  subprocess.call(cmd)

def get_clsfile(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.ClassDownloader")
  cmd.append(argv[2])
  cmd.append(argv[3])
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  subprocess.call(cmd)

def terminate():
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.DFSTerminator")
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  cmd.append(config['fsImage dir'])
  subprocess.call(cmd)

if __name__ == "__main__":
  dfs(sys.argv)
