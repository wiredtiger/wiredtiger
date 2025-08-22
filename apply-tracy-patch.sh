#!/bin/bash

# Quick Tracy Integration Patch Application Script
# Usage: ./apply-tracy-patch.sh

set -e

echo "Applying Tracy profiler integration patch..."

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "Error: Not in a git repository"
    exit 1
fi

# Check if patch file exists
if [ ! -f "tracy-integration.patch" ]; then
    echo "Error: tracy-integration.patch not found"
    echo "Make sure you have the patch file in the current directory"
    exit 1
fi

# Apply the patch
echo "Applying patch..."
git apply tracy-integration.patch

echo "✅ Tracy integration patch applied successfully!"
echo ""
echo "To build with Tracy profiling enabled:"
echo "  mkdir build-tracy && cd build-tracy"
echo "  cmake -DTRACY_ENABLE=ON .."
echo "  make -j\$(nproc)"
echo ""
echo "To use Tracy in your code:"
echo "  #include \"tracy/Tracy.hpp\""
echo "  ZoneScoped; // Add this to functions you want to profile"
