import argparse
import subprocess
import os
import shutil
import time
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_name',
                        help='test executable',
                        required=True)
    parser.add_argument('-j', '--parallel',
                    help='run parallel',
                    required=True)
    parser.add_argument('-t', '--tries',
                    help='number of tries',
                    type=int, default=1)
    args = parser.parse_args()
  
    if (os.path.exists("smart_configs")):
      shutil.rmtree("smart_configs")

    os.mkdir("smart_configs")
    shutil.copyfile(args.config_name, "smart_configs/base")

    print(args.tries)
    format_recursion("base", sys.maxsize, args.tries)

def format_recursion(base_config, max_time, tries):

  return_code = subprocess.call("python generate_format_cfg.py -n 5 -i smart_configs/{0} -o smart_configs/{0}_test".format(base_config), shell=True)
  if (return_code != 0):
    print("not working")
    exit(-1)

  for config in os.listdir("smart_configs"):
    if config.startswith("{0}_test".format(base_config)):
      for _ in range(0, tries):
        st = time.time()
        test_command = "./t -c smart_configs/{0}".format(config)
        return_code = subprocess.call(test_command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        et = time.time()
        print(return_code, et - st, config)
        if (return_code != 0 and et - st < max_time):
          format_recursion(config, et - st, tries)
          break
        


if __name__ == "__main__":
    main()