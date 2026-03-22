import argparse
import json
import logging
from collections import deque
from dataclasses import asdict
from pathlib import Path
from typing import Any

import grpc

from wt_decode.disagg.models import PageTuple, DecodedPageInfo
from wt_decode.disagg.client import create_page_service_stub, fetch_page, decrypt_full_response_json, decrypt_response_deltas
from wt_decode.disagg.decoding import make_decode_opts, decode_page_bytes, get_page_type_name, extract_children, capture_page_text
from wt_decode.output.text import print_page as _print_page
from wt_decode.ui.rich_page import rich_print_page

logger = logging.getLogger(__name__)

def page_to_json_dict(page: Any) -> dict[str, Any]:
    """Convert a protobuf Page message to a JSON-serializable dict for artifact storage."""
    import base64
    return {
        "log_id": page.log_id,
        "table_id": page.table_id,
        "page_id": page.page_id,
        "lsn": page.lsn,
        "contents_b64": base64.b64encode(page.contents).decode("ascii"),
        "contents_size": len(page.contents),
        "deltas_b64": [base64.b64encode(d).decode("ascii") for d in page.deltas],
        "delta_sizes": [len(d) for d in page.deltas],
        "base_lsn": page.base_lsn,
        "lsns": list(page.lsns),
        "backlinks": list(page.backlinks),
        "full_image_lsn": page.full_image_lsn,
        "full_image_backlink_lsn": page.full_image_backlink_lsn,
    }

def traverse_tree(args: argparse.Namespace) -> int:
    """Main traversal loop: fetch -> decrypt -> decode -> find children -> repeat."""
    # decode_opts targets the base (full) image – is_delta=False ensures cell parsing
    # uses the standard (non-delta) format even when the underlying response contains
    # delta blobs.  decrypt_full_response_json always returns the decrypted base image
    # bytes, so this pairing is always correct.
    decode_opts = make_decode_opts(
        verbose=args.verbose,
        bson=args.bson,
        disagg=True,
        debug=args.debug,
        is_delta=False,
    )
    # Separate opts for decoding individual disagg delta pages.
    delta_decode_opts = make_decode_opts(
        verbose=args.verbose,
        bson=args.bson,
        disagg=True,
        debug=args.debug,
        is_delta=True,
    )

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(f"/tmp/disagg_tree_{args.table_id}_{args.root_page_id}_{args.root_lsn}")
    )
    pages_dir = output_dir / "pages"
    decrypted_dir = output_dir / "decrypted"
    decoded_dir = output_dir / "decoded"
    for d in (pages_dir, decrypted_dir, decoded_dir):
        d.mkdir(parents=True, exist_ok=True)

    stub = create_page_service_stub(args.page_server)
    root = PageTuple(page_id=args.root_page_id, lsn=args.root_lsn)
    queue: deque[PageTuple] = deque([root])
    visited: set[PageTuple] = set()
    manifest_pages: list[dict[str, Any]] = []

    print(
        f"Starting tree fetch from root page_id={root.page_id}, lsn={root.lsn}, "
        f"table_id={args.table_id}, log_id={args.log_id}"
    )
    print(f"Artifacts will be written to: {output_dir}")

    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        if args.max_pages and len(visited) >= args.max_pages:
            print(f"Reached --max-pages={args.max_pages}, stopping early.")
            break

        print(f"\nFetching page_id={current.page_id} lsn={current.lsn} ...")
        visited.add(current)

        try:
            response = fetch_page(stub, args.log_id, args.table_id, current.page_id, current.lsn)
        except grpc.RpcError as exc:
            logger.error("gRPC error fetching page_id=%d lsn=%d: %s", current.page_id, current.lsn, exc)
            continue

        page_proto = response.page
        page_json_path = pages_dir / f"page_{current.page_id}_lsn_{current.lsn}.json"
        page_json_path.write_text(json.dumps(page_to_json_dict(page_proto), indent=2))

        decrypted_path = decrypted_dir / f"decrypted_{args.log_id}_{args.table_id}_{current.page_id}_{current.lsn}.bin"
        # decrypt_full_response_json returns the decrypted base image bytes. When the
        # response contains deltas the decryptor overwrites --outputPath for each call,
        # so the artifact file ends up holding the last delta; base_bytes is captured
        # before that overwrite and is the correct input for page decoding / child
        # extraction.
        base_bytes = decrypt_full_response_json(
            args.decryptor_path, args.key_file,
            response,
            current.lsn, args.table_id, current.page_id,
            output_path=decrypted_path,
            debug=args.debug,
        )
        decoded_page = decode_page_bytes(base_bytes, decode_opts)
        decoded_text = capture_page_text(decoded_page, decode_opts)

        decoded_path = decoded_dir / f"decoded_page_{current.page_id}_lsn_{current.lsn}.txt"
        decoded_path.write_text(decoded_text)

        if getattr(args, "rich", False):
            rich_print_page(args.table_id, current.page_id, current.lsn, args.log_id, decoded_page)
        else:
            print(f"\n--- Page: id={args.table_id}, page={current.page_id}, lsn={current.lsn} ---")
            _print_page(decoded_page, split=getattr(decode_opts, 'split', False),
                        decode_as_bson=getattr(decode_opts, 'bson', False),
                        disagg=getattr(decode_opts, 'disagg', True))

        children = extract_children(decoded_page)
        for child in children:
            child_tuple = PageTuple(page_id=child["page_id"], lsn=child["lsn"])
            if child_tuple not in visited:
                queue.append(child_tuple)

        # Decode each delta page individually with is_delta=True decode opts.
        decoded_delta_paths: list[str] = []
        decrypted_delta_paths: list[str] = []
        if page_proto.deltas:
            delta_pairs = decrypt_response_deltas(
                args.decryptor_path, args.key_file,
                response,
                current.lsn, args.table_id, current.page_id,
                log_id=args.log_id,
                output_dir=decrypted_dir,
                debug=args.debug,
            )
            for d_lsn, d_bytes, d_decrypted_path in delta_pairs:
                if d_decrypted_path is not None:
                    decrypted_delta_paths.append(str(d_decrypted_path))
                try:
                    delta_page = decode_page_bytes(d_bytes, delta_decode_opts)
                    delta_text = capture_page_text(delta_page, delta_decode_opts)
                    delta_path = decoded_dir / f"decoded_delta_{current.page_id}_lsn_{d_lsn}.txt"
                    delta_path.write_text(delta_text)
                    decoded_delta_paths.append(str(delta_path))
                    if getattr(args, "rich", False):
                        rich_print_page(args.table_id, current.page_id, d_lsn, args.log_id, delta_page)
                    else:
                        print(f"  [delta] lsn={d_lsn} type={get_page_type_name(delta_page)}")
                        _print_page(delta_page, split=getattr(delta_decode_opts, 'split', False),
                                    decode_as_bson=getattr(delta_decode_opts, 'bson', False),
                                    disagg=getattr(delta_decode_opts, 'disagg', True))
                except Exception as exc:
                    logger.warning(
                        "Failed to decode delta page_id=%d lsn=%d: %s",
                        current.page_id, d_lsn, exc,
                    )

        info = DecodedPageInfo(
            page_id=current.page_id,
            lsn=current.lsn,
            page_type=get_page_type_name(decoded_page),
            write_gen=decoded_page.page_header.write_gen if decoded_page.page_header else None,
            decrypted_size=decrypted_path.stat().st_size,
            num_children=len(children),
            children=children,
            page_json_path=str(page_json_path),
            decrypted_path=str(decrypted_path),
            decoded_path=str(decoded_path),
            has_deltas=bool(page_proto.deltas),
            num_deltas=len(page_proto.deltas),
            decoded_delta_paths=decoded_delta_paths,
            decrypted_delta_paths=decrypted_delta_paths,
        )
        manifest_pages.append(asdict(info))

        print(
            f"  -> type={info.page_type}, write_gen={info.write_gen}, "
            f"decrypted_size={info.decrypted_size}, "
            f"children={len(children)}, deltas={info.num_deltas}"
        )

    manifest = {
        "root": {
            "log_id": args.log_id,
            "table_id": args.table_id,
            "page_id": args.root_page_id,
            "lsn": args.root_lsn,
        },
        "pages_visited": len(manifest_pages),
        "output_dir": str(output_dir),
        "pages": manifest_pages,
    }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"\nDone. Visited {len(manifest_pages)} pages.")
    print(f"Manifest: {manifest_path}")
    return 0
