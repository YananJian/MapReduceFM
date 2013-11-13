import json
import subprocess
import sys

def start_namenode(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.NameNodeImpl")
  cmd.append(config['replication'])
  cmd.append(config['healthcheck interval'])
  cmd.append(config['block size'])
  cmd.append(config['dfs_registry']['port'])
  cmd.append(str(len(config['datanode'])))
  cmd.append(config['fsImage dir'])
  cmd.append(config['namenode port'])
  subprocess.call(cmd)

if __name__ == "__main__":
  start_namenode(sys.argv)
