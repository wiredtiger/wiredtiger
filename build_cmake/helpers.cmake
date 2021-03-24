#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
#  See the file LICENSE for redistribution information
#

cmake_minimum_required(VERSION 3.11.0)

# Helper function for evaluating a list of dependencies. Mostly used by the
# "config_X" helpers to evaluate the dependencies required to enable the config
# option.
#   depends - a list (semicolon seperated) of dependencies to evaluate
#   enabled - name of the output variable set with either 'ON' or 'OFF' (based
#             on evaluated dependencies). Output variable is set in the callers scope.
function(eval_dependency depends enabled)
    # If no dependencies were given then we default to an enabled state
    if(("${depends}" STREQUAL "") OR ("${depends}" STREQUAL "NOTFOUND"))
        set(enabled ON PARENT_SCOPE)
        return()
    endif()
    # Evaluate each dependency
    set(is_enabled ON)
    foreach(dependency ${depends})
        string(REGEX REPLACE " +" ";" dependency "${dependency}")
        if(NOT (${dependency}))
            set(is_enabled OFF)
            break()
        endif()
    endforeach()
    set(enabled ${is_enabled} PARENT_SCOPE)
endfunction()

# config_string(config_name description DEFAULT <default string> [DEPENDS <deps>] [INTERNAL])
# Defines a string configuration option. The configuration option is stored in the cmake cache
# and can be exported to the wiredtiger config header.
#   config_name - name of the configuration option
#   description - docstring to describe the configuration option (viewable in the cmake-gui)
#   DEFAULT <default string> -  Default value of the configuration string. Used when not manually set
#       by a cmake script or in the cmake-gui.
#   DEPENDS <deps> - list of dependencies (semicolon seperated) required for the configuration string
#       to be present and set in the cache. If any of the dependencies aren't met, the
#       configuration value won't be present in the cache.
#   INTERNAL - hides the configuration option from the cmake-gui by default. Useful if you don't want
#       to expose the variable by default to the user i.e keep it internal to the implementation
#       (but still need it in the cache).
function(config_string config_name description)
    cmake_parse_arguments(
        PARSE_ARGV
        2
        "CONFIG_STR"
        "INTERNAL"
        "DEFAULT;DEPENDS"
        ""
    )

    if (NOT "${CONFIG_STR_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Unknown arguments to config_str: ${CONFIG_STR_UNPARSED_ARGUMENTS}")
    endif()
    # We require a default value (not optional)
    if ("${CONFIG_STR_DEFAULT}" STREQUAL "")
        message(FATAL_ERROR "No default value passed")
    endif()

    # Check that the configs dependencies are enabled before setting it to a visible enabled state
    eval_dependency("${CONFIG_STR_DEPENDS}" enabled)
    set(default_value "${CONFIG_STR_DEFAULT}")
    if(enabled)
        # Set an internal cache variable "${config_name}_DISABLED" to capture its enabled/disabled state
        # We want to ensure we capture a transition from a disabled to enabled state when dependencies are met
        if(${config_name}_DISABLED)
            unset(${config_name}_DISABLED CACHE)
            set(${config_name} ${default_value} CACHE STRING "${description}" FORCE)
        else()
            set(${config_name} ${default_value} CACHE STRING "${description}")
        endif()
        if (CONFIG_STR_INTERNAL)
            # Mark as an advanced variable, hiding it from initial UI's views
            mark_as_advanced(FORCE ${config_name})
        endif()
    else()
        # Config doesn't meet dependency requirements, remove it from the cache and flag it as disabled.
        unset(${config_name} CACHE)
        set(${config_name}_DISABLED ON CACHE INTERNAL "" FORCE)
    endif()
endfunction()

# config_choice(config_name description OPTIONS <opts>)
# Defines a configuration option, bounded with pre-set toggleable values. The configuration option is stored
# in the cmake cache and can be exported to the wiredtiger config header. We default to the first *available* option in the
# list if the config has not been manually set by a cmake script or in the cmake-gui.
#   config_name - name of the configuration option
#   description - docstring to describe the configuration option (viewable in the cmake-gui)
#   OPTIONS - a list of option values that the configuration option can be set to. Each option is itself a semicolon
#       seperated list consisting of "<option-name>;<config-name>;<option-dependencies>".
#       * option-name: name of the given option stored in the ${config_name} cache variable and presented
#           to users in the gui (usually something understandable)
#       * config-name: an additional cached configuration variable that is made available if the option is selected.
#           It is only present if the option is chosen, otherwise it is unset.
#       *  option-dependencies: dependencies required for the option to be made available. If its dependencies aren't met
#           the given option will become un-selectable.
function(config_choice config_name description)
    cmake_parse_arguments(
        PARSE_ARGV
        2
        "CONFIG_OPT"
        ""
        ""
        "OPTIONS"
    )

    if (NOT "${CONFIG_OPT_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Unknown arguments to config_opt: ${CONFIG_OPT_UNPARSED_ARGUMENTS}")
    endif()
    # We require option values (not optional)
    if ("${CONFIG_OPT_OPTIONS}" STREQUAL "")
        message(FATAL_ERROR "No options passed")
    endif()

    set(found_option ON)
    set(found_pre_set OFF)
    set(default_config_field "")
    set(default_config_var "")
    foreach(curr_option ${CONFIG_OPT_OPTIONS})
        list(LENGTH curr_option opt_length)
        if (NOT opt_length EQUAL 3)
            message(FATAL_ERROR "Invalid option format: ${curr_option}")
        endif()
        # We expect three items defined for each option
        list(GET curr_option 0 option_config_field)
        list(GET curr_option 1 option_config_var)
        list(GET curr_option 2 option_depends)
        # Check that the options dependencies are enabled before setting it to a selectable state
        eval_dependency("${option_depends}" enabled)
        if(enabled)
            list(APPEND all_option_config_fields ${option_config_field})
            # The first valid/selectable option found will be the default config value.
            if (found_option)
                set(found_option OFF)
                set(default_config_field "${option_config_field}")
                set(default_config_var "${option_config_var}")
            endif()
            # Check if the option is already set with this given field. We don't want to override the configs value
            # with a default value if its already been pre-set in the cache e.g. by early config scripts.
            if("${${config_name}}" STREQUAL "${option_config_field}")
                set(${option_config_var} ON CACHE INTERNAL "" FORCE)
                set(${config_name}_CONFIG_VAR ${option_config_var} CACHE INTERNAL "" FORCE)
                set(found_pre_set ON)
                set(found_option OFF)
                set(default_config_field "${option_config_field}")
                set(default_config_var "${option_config_var}")
            else()
                # Clear the cache of the current set value
                set(${option_config_var} OFF CACHE INTERNAL "" FORCE)
            endif()
        else()
            unset(${option_config_var} CACHE)
            # Check if the option is already set with this given field - we want to clear it if so
            if ("${${config_name}_CONFIG_VAR}" STREQUAL "${option_config_var}")
                unset(${config_name}_CONFIG_VAR CACHE)
            endif()
            if("${${config_name}}" STREQUAL "${option_config_field}")
                unset(${config_name} CACHE)
            endif()
        endif()
    endforeach()
    # If the config hasn't been set we can load it with the default option found earlier.
    if(NOT found_pre_set)
        set(${default_config_var} ON CACHE INTERNAL "" FORCE)
        set(${config_name} ${default_config_field} CACHE STRING ${description})
        set(${config_name}_CONFIG_VAR ${default_config_var} CACHE INTERNAL "" FORCE)
    endif()
    set_property(CACHE ${config_name} PROPERTY STRINGS ${all_option_config_fields})
endfunction()

# config_bool(config_name description DEFAULT <default-value> [DEPENDS <deps>] [DEPENDS_ERROR <config-val> <error-string>])
# Defines a boolean (ON/OFF) configuration option . The configuration option is stored in the cmake cache
# and can be exported to the wiredtiger config header.
#   config_name - name of the configuration option
#   description - docstring to describe the configuration option (viewable in the cmake-gui)
#   DEFAULT <default-value> -  default value of the configuration bool (ON/OFF). Used when not manually set
#       by a cmake script or in the cmake-gui or when dependencies aren't met.
#   DEPENDS <deps> - list of dependencies (semicolon seperated) required for the configuration bool
#       to be set to the desired value. If any of the dependencies aren't met the configuration value
#       will be set to its default value.
#   DEPENDS_ERROR <config-val> <error-string> - specifically throw a fatal error when the configuration option is set to
#       <config-val> despite failing on its dependencies. This is mainly used for commandline-like options where you want
#       to signal a specific error to the caller when dependencies aren't met e.g. toolchain is missing library (as opposed to
#       silently defaulting).
function(config_bool config_name description)
    cmake_parse_arguments(
        PARSE_ARGV
        2
        "CONFIG_BOOL"
        ""
        "DEFAULT;DEPENDS"
        "DEPENDS_ERROR"
    )

    if(NOT "${CONFIG_BOOL_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Unknown arguments to config_bool: ${CONFIG_BOOL_UNPARSED_ARGUMENTS}")
    endif()
    # We require a default value (not optional)
    if("${CONFIG_BOOL_DEFAULT}" STREQUAL "")
        message(FATAL_ERROR "No default value passed")
    endif()

    set(depends_err_value)
    set(depends_err_message "")
    # If DEPENDS_ERROR is specifically set, parse the value we want to throw an error on if the dependency fails
    if(CONFIG_BOOL_DEPENDS_ERROR)
        list(LENGTH CONFIG_BOOL_DEPENDS_ERROR depends_error_length)
        if(NOT depends_error_length EQUAL 2)
            message(FATAL_ERROR "Invalid usage of DEPENDS_ERROR: requires <Error Value> <Error Message>")
        else()
            list(GET CONFIG_BOOL_DEPENDS_ERROR 0 err_val)
            if(err_val)
                set(depends_err_value "1")
            else()
                set(depends_err_value "0")
            endif()
            list(GET CONFIG_BOOL_DEPENDS_ERROR 1 depends_err_message)
        endif()
    endif()

    # Check that the configs dependencies are enabled before setting it to a visible enabled state
    eval_dependency("${CONFIG_BOOL_DEPENDS}" enabled)
    if(enabled)
        # Set an internal cache variable "${config_name}_DISABLED" to capture its enabled/disabled state
        # We want to ensure we capture a transition from a disabled to enabled state when dependencies are met
        if(${config_name}_DISABLED)
            unset(${config_name}_DISABLED CACHE)
            set(${config_name} ${CONFIG_BOOL_DEFAULT} CACHE STRING "${description}" FORCE)
        else()
            set(${config_name} ${CONFIG_BOOL_DEFAULT} CACHE STRING "${description}")
        endif()
    else()
        set(config_value "0")
        if (${${config_name}})
            set(config_value "1")
        endif()
        # If the user tries to set the config option to a given value when its dependencies
        # are not met, throw an error (when DEPENDS_ERROR is explicitly set)
        if(CONFIG_BOOL_DEPENDS_ERROR)
            if(${depends_err_value} EQUAL ${config_value})
                message(FATAL_ERROR "Unable to set ${config_name}: ${depends_err_message}")
            endif()
        endif()
        # Config doesn't meet dependency requirements, set its default state and flag it as disabled.
        set(${config_name} ${CONFIG_BOOL_DEFAULT} CACHE STRING "${description}" FORCE)
        set(${config_name}_DISABLED ON CACHE INTERNAL "" FORCE)
    endif()
endfunction()
