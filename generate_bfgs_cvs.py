#!/usr/bin/env python3
import csv
from pathlib import Path

from bb_client import BuildBaronClient, BBSearchBfgsSpec, get_oauth_credentials
from bb_client.models.bfg import AttributeType

BF_KEY = "WT-16531"
OUT_CSV = Path("bfgs.csv")

def get_client():
    """
    Get an authenticated BuildBaronClient using OAuth.

    This will open a browser on first run by default. If you're on a headless VM,
    see the README: you can use get_oauth_credentials(print_url=True) and port-forward
    8989, or use get_client_cred_oauth_credentials(...) for a machine user.
    """
    oauth_credentials = get_oauth_credentials()
    return BuildBaronClient.new_client(oauth_credentials)

def main():
    client = get_client()

    # Search all BFGs linked to this BF key.
    spec = BBSearchBfgsSpec(
        bf_key=BF_KEY,
        page_size=1000,  # big page size so we don't make many calls
    )

    rows = []
    for bfg in client.search_bfgs(spec):
        # bfg is a bb_client.models.bfg.Bfg
        bfg_id = bfg.get_key()  # from attributes["key"]
        task_id = bfg.task_id
        full_commit = bfg.attributes.get(AttributeType.BASE_COMMIT)
        if not bfg_id or not task_id or not full_commit:
            continue

        short_commit = full_commit[:8]
        rows.append(
            {
                "bfg_id": bfg_id,
                "task_id": task_id,
                "commit": short_commit,
            }
        )

    if not rows:
        print(f"No BFGs found for {BF_KEY}")
        return

    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["bfg_id", "task_id", "commit"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_CSV.resolve()}")

if __name__ == "__main__":
    main()