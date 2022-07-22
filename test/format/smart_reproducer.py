import argparse
import subprocess
import os
import shutil
import time
import sys
import threading
import generate_format_cfg as gen_format_cfg

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

    best_config_dict = {}
    best_config_dict['result'] = 0
    best_config_dict['frequency'] = 0
    best_config_dict['max_time'] = sys.maxsize
    format_recursion("base", args.tries, args.parallel, best_config_dict)
    print(best_config_dict)

def thread_run_test_format_func(test_command, thread_return, tries, best_config_dict):
  failed = 0
  max_time = 0
  frequency = 0
  for i in range(0, tries):
    st = time.time()
    return_code = subprocess.call(test_command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    et = time.time()
    print("I am in {0} iteration out of {1}\n".format(i, tries))
    if (return_code != 0):
      failed = return_code
      max_time = max_time + et - st
      frequency = frequency + 1

  if (failed != 0):
    if (frequency > best_config_dict['frequency']):
      thread_return['result'] = failed
      thread_return['max_time'] = max_time
      thread_return['frequency'] = frequency
    elif (frequency == best_config_dict['frequency'] and max_time < best_config_dict['max_time']):
      thread_return['result'] = failed
      thread_return['max_time'] = max_time
      thread_return['frequency'] = frequency

def format_recursion(base_config, tries, parallel, best_config_dict):
  for config in os.listdir("."):
    if config.startswith("dump_t."):
      os.remove(config)


  change_list = gen_format_cfg.my_func("smart_configs/base", "smart_configs/{0}_test".format(base_config), true, config_dict_range, false)
  print(change_list)

  threads = list()
  thread_id = 0
  for config in os.listdir("smart_configs"):
    if config.startswith("{0}_test".format(base_config)):
      test_command = "./t -h RUNDIR.{0} -c smart_configs/{1}".format(thread_id, config)

      if (parallel):
        thread_return = {}
        thread_return['result'] = 0
        thread_return['max_time'] = 0
        thread_return['frequency'] = 0
    
        x = threading.Thread(target=thread_run_test_format_func, args=(test_command,thread_return,tries,best_config_dict))
        print("Thread {0}: Starting test format\n".format(thread_id))
        x.start()
        threads.append((x, config, thread_return))
        thread_id = thread_id + 1
      else:
        frequency = 0
        failed = 0
        max_time = 0
        for _ in range(0, tries):
          st = time.time()
          return_code = subprocess.call(test_command, shell=True)
          et = time.time()
          if (return_code != 0):
            failed = return_code
            max_time = max_time + et - st
            frequency = frequency + 1

        if (failed != 0):
          if (frequency > best_config_dict['frequency']):
            best_config_dict['frequency'] = frequency
            best_config_dict['max_time'] = max_time
            best_config_dict['result'] = failed
            format_recursion(config, tries, parallel, best_config_dict)
          elif (frequency == best_config_dict['frequency'] and max_time < best_config_dict['max_time']):
            best_config_dict['frequency'] = frequency
            best_config_dict['max_time'] = max_time
            best_config_dict['result'] = failed
            format_recursion(config, tries, parallel, best_config_dict)
        

  if (parallel):
    for (t, config, thread_ret) in threads:
      t.join()
      print("Waiting for test format to finish test format\n")
      # Result 
      if (thread_ret['result'] != 0):
        print(thread_ret['result'], thread_ret['max_time'], best_config_dict)
        if (thread_ret['frequency'] > best_config_dict['frequency']):
          best_config_dict['frequency'] = thread_ret['frequency']
          best_config_dict['max_time'] = thread_ret['max_time']
          best_config_dict['result'] = thread_ret['result']
        elif (thread_ret['frequency'] == best_config_dict['frequency'] and thread_ret['max_time'] < best_config_dict['max_time']):
          best_config_dict['frequency'] = thread_ret['frequency']
          best_config_dict['max_time'] = thread_ret['max_time']
          best_config_dict['result'] = thread_ret['result']
        
    for (t, config, thread_ret) in threads:
      if (thread_ret['result'] != 0):
        format_recursion(config, tries, parallel, best_config_dict)

if __name__ == "__main__":
    main()