import argparse
import random
import re
import sys

# TODO - Ideas
# I need to return the changes done for each config.
# Dict of dict
# First key is the config name
# Then I have:
# {
#   field_1: [prev_value, new_value],
#   field_2: [prev_value, new_value]
# }

def my_func(input_config, output_config, num_generated_configs, unique, dictionary, verbose):
    """
    Return a dictionary with the config name and the changes.
    """

    assert dictionary is not None

    # All the fields to play on.
    fields = list(dictionary.keys())
    # Array that contains the name of each new config.
    new_configs = []
    # Array that contains the field to change in the config at the same index.
    field_to_change = []
    # Indicate if a config has been modified.
    config_modified = [False] * num_generated_configs
    # Dictionary of changes.
    changes = {}

    for num in range(num_generated_configs):
        # Create each new config.
        config_name = output_config + "_" + str(num)
        new_configs.append(open(config_name, "w"))

        # Associate a new entry with the generated config.
        changes[config_name] = {}

        if unique:
            # Select a random field to modify for the config.
            field_to_change.append(random.choice(fields))
            if verbose:
                print("Field to change for config " + str(num) + " is " + field_to_change[num])

    # Read the input configuration file.
    config = open(input_config, "r")
    lines = config.readlines()

    for idx, line in enumerate(lines):

        written = False
        line = line.strip()

        if unique:
            indexes = []
            for idx, field in enumerate(field_to_change):
                if config_modified[idx] is False and \
                    (line.startswith(field) or re.search("(^table\d.*\.)" + field, line)):
                    indexes.append(idx)

            # Generate the new value for each selected config.
            for idx, new_config in enumerate(new_configs):
                if idx in indexes:
                    values = dictionary[field_to_change[idx]]
                    # Generate a random value
                    new_value = None
                    field_value = line.split('=')
                    current_field = field_value[0]
                    current_value = field_value[1]
                    # We want to make sure the new value is not the same as the input configuration.
                    # TODO - Maybe we don't want to make this mandatory.
                    while new_value is None or current_value == new_value:
                        if isinstance(values[0], str):
                            new_value = random.choice(values)
                        elif isinstance(values[0], int):
                            new_value = random.randint(values[0], values[1])
                        else:
                            sys.exit("Error type", type(values[0]))

                    if verbose:
                        print("new value", new_value)

                    new_line = current_field + "=" + str(new_value)
                    new_config.write(new_line + "\n")
                    # The field might appear for different tables in the configurations. Since we want
                    # to change all of them, don't mark the config as modified otherwise we will change
                    # only the value for one table.
                    # config_modified[idx] = True

                    # Save the changes we made.
                    changes[new_config.name][current_field] = [current_value, new_value]
                else:
                    # Write the same line.
                    new_config.write(line + "\n")
        else:

            for key in dictionary:

                # It does not always start with the field. We may have "table[0-9]" in the front.
                # i.e table4.runs.type=row-store
                if line.startswith(key) or re.search("(^table\d.*\.)" + key, line):

                    if verbose:
                        print("Found field:", line)

                    values = dictionary[key]

                    field_value = line.split('=')
                    current_field = field_value[0]
                    current_value = field_value[1]

                    # TODO - Do we want to write to all files?
                    # Generate a different value for each new config.
                    for new_config in new_configs:

                        # We either generate a number using the given range or choose an element among the
                        # list. We use the type of the first element among the choices to determine what
                        # needs to be done.
                        new_value = None
                        if isinstance(values[0], str):
                            new_value = random.choice(values)
                        elif isinstance(values[0], int):
                            new_value = random.randint(values[0], values[1])
                        else:
                            sys.exit("Error type", type(values[0]))
                        
                        if verbose:
                            print("new value", new_value)

                        new_line = current_field + "=" + str(new_value)
                        new_config.write(new_line + "\n")

                        # Save the change we made.
                        changes[new_config.name][current_field] = [current_value, new_value]

                    written = True
                    break

            # The line is unmodified, write it to all files.
            if written is False:
                for new_config in new_configs:
                    new_config.write(line + "\n")

    # Close all files.
    for new_config in new_configs:
        new_config.close()
    config.close()

    return changes

if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, allow_abbrev=True)
    parser.add_argument("-i", "--input", help="Input configuration", default="CONFIG.stress")
    parser.add_argument("-o", "--output", help="Output configuration", default="CONFIG_NEW")
    parser.add_argument("-n", "--number", help="Number of generated configurations", type=int,
        default=1)
    parser.add_argument("-u", "--unique", help="One unique change per configuration",
        action="store_true", default=False)
    parser.add_argument("-v", "--verbose", help="Verbose", action="store_true", default=False)

    args = parser.parse_args()
    input_config = args.input
    num_generated_configs = args.number
    output_config = args.output
    unique = args.unique
    verbose = args.verbose

    if verbose:
        print(args)

    # TODO - If one of these fields was change before and led to a quicker reproducer, do we want to:
    # - Keep the same range?
    # - Modify the field at all?
    dictionary = {
        # 'backup.incremental': ["on", "off"],
        # 'block_cache': [0, 1000],
        # 'cache' : [0,1000],
        # 'btree.compression': ["none", "zlib"],
        'btree.split_pct': [0, 10],
        'runs.rows': [0, 2200000],
        # 'runs.threads': [1, 10],
        # 'runs.type': ["variable-length column-store", "row-store"],
        }

    changes = my_func(input_config, output_config, num_generated_configs, unique, dictionary, verbose)
    print(changes)
