#!/usr/bin/env bash
##############################################################################################
# Check branches to ensure forward/backward compatibility, including some upgrade/downgrade testing.
##############################################################################################

set -e
. ./compatibility_test_for_releases_lib.sh

# Create a directory in which to do the work.
top="test-compatibility-run"
rm -rf "$top" && mkdir "$top"
cd "$top"

# Build the branches.
(build_branch mongodb-4.0)
(build_branch mongodb-4.2)
(build_branch mongodb-4.4)
(build_branch develop)

# Run format in each branch for supported access methods.
(run_format mongodb-4.0 "fix row var")
(run_format mongodb-4.2 "fix row var")
(run_format mongodb-4.4 "row")
(run_format develop "row")

# Verify backward compatibility for supported access methods.
(verify_branches mongodb-4.2 mongodb-4.0 "fix row var")
(verify_branches mongodb-4.4 mongodb-4.2 "fix row var")
(verify_branches develop mongodb-4.4 "row")
(verify_branches develop mongodb-4.2 "row")

# Verify forward compatibility for supported access methods.
(verify_branches mongodb-4.2 mongodb-4.4 "row")
(verify_branches mongodb-4.2 develop "row")
(verify_branches mongodb-4.4 develop "row")

# Upgrade/downgrade testing for supported access methods.
(upgrade_downgrade mongodb-4.2 mongodb-4.4 "row")
(upgrade_downgrade mongodb-4.2 develop "row")
(upgrade_downgrade mongodb-4.4 develop "row")

exit 0
