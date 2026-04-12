#!/usr/bin/env bash
# Install the curated set of requirements needed for devcontainer setup.
cd requirements || exit 1

curated_requirements=(
  "default.txt"
  "test.txt"
  "test-core.txt"
  "pkgutils.txt"
)

for requirement_file in "${curated_requirements[@]}"; do
  if [ -f "${requirement_file}" ]; then
    pip install -r "${requirement_file}"
  fi
done

if [ "${INSTALL_OPTIONAL_REQUIREMENTS:-0}" = "1" ] && [ -d "extras" ]; then
  for requirement_file in extras/*.txt; do
    if [ -f "${requirement_file}" ]; then
      pip install -r "${requirement_file}"
    fi
  done
fi