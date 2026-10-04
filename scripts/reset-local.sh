#!/usr/bin/env bash
# Wipes the local lake and every checkpoint. Needed more often than it should
# be: changing the silver schema invalidates the checkpoint and Spark then
# refuses to start with a message that does not say so.
set -euo pipefail
cd "$(dirname "$0")/.."

read -rp "delete all local lake data and checkpoints? [y/N] " ok
[[ "$ok" == "y" ]] || exit 0

docker compose down -v
rm -rf spark-warehouse metastore_db derby.log
echo "gone. 'make up' then 'make produce' to refill."
