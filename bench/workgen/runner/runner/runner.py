import argparse, os, shutil

def run_workgen(workload_func, register_args_func=None):
    parser = argparse.ArgumentParser(description="Execute workload.")
    parser.add_argument("-K", dest="keep", action="store_true",
                        help="Retain the WT_TEST dir")
    if register_args_func is not None:
        register_args_func(parser)
    args = parser.parse_args()
    if not args.keep:
        # Clear out the WT_TEST directory.
        shutil.rmtree('WT_TEST', True)
        os.mkdir('WT_TEST')
    workload_func(args)
