import argparse
import subprocess
import os
import shutil
import time
import sys
import threading

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_name',
                        help='test executable',
                        required=True)
    parser.add_argument("-j", "--parallel", help="Run parallel",
                    action="store_true", default=False)
    parser.add_argument('-t', '--tries',
                    help='number of tries',
                    type=int, default=1)
    args = parser.parse_args()
  
    if (os.path.exists("smart_configs")):
      shutil.rmtree("smart_configs")

    os.mkdir("smart_configs")
    shutil.copyfile(args.config_name, "smart_configs/base")

    format_recursion("base", sys.maxsize, args.tries, args.parallel)

def thread_run_test_format_func(test_command, thread_return, max_time, tries):
  for _ in range(0, tries):
    st = time.time()
    return_code = subprocess.call(test_command, shell=True)
    et = time.time()
    if (return_code != 0 and et - st < max_time):
      thread_return['result'] = return_code
      thread_return['max_time'] = et - st
      break

def format_recursion(base_config, max_time, tries, parallel):
  for config in os.listdir("."):
    if config.startswith("dump_t."):
      os.remove(config)

  return_code = subprocess.call("python generate_format_cfg.py -n 5 -i smart_configs/{0} -o smart_configs/{0}_test".format(base_config), shell=True)
  if (return_code != 0):
    print("not working")
    exit(-1)

  threads = list()
  thread_id = 0
  for config in os.listdir("smart_configs"):
    if config.startswith("{0}_test".format(base_config)):
      test_command = "./t -h RUNDIR.{0} -c smart_configs/{1}".format(thread_id, config)

      if (parallel):
        thread_return = {}
        thread_return['result'] = 0
        thread_return['max_time'] = max_time
    
        x = threading.Thread(target=thread_run_test_format_func, args=(test_command,thread_return,max_time,tries))
        x.start()
        threads.append((x, config, thread_return))
        thread_id = thread_id + 1
      else:
        for _ in range(0, tries):
          st = time.time()
          return_code = subprocess.call(test_command, shell=True)
          et = time.time()
          if (return_code != 0 and et - st < max_time):
            print(return_code, et - st, config)
            format_recursion(config, et - st, tries, parallel)
            break

  if (parallel):
    for (t, config, ret) in threads:
        t.join()
        if (thread_return['result'] != 0):
          print(thread_return.result, thread_return['max_time'], config)
          format_recursion(config, thread_return['max_time'], tries, parallel)

if __name__ == "__main__":
    main()