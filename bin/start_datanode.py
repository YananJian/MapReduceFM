import json
import subprocess
import sys

def start_datanode(argv):
  with open("../conf/config.json", "r") as infile:
    config = json.load(infile)
  cmd = list()
  cmd.append("java")
  cmd.append("dfs.DataNodeImpl")
  cmd.append(argv[1])
  cmd.append(config['datanode'][int(argv[1])]['dir'])
  cmd.append(config['datanode'][int(argv[1])]['port'])
  cmd.append(config['dfs_registry']['host'])
  cmd.append(config['dfs_registry']['port'])
  subprocess.call(cmd)

if __name__ == "__main__":
  start_datanode(sys.argv)
