import base64
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, List

import grpc
from google.protobuf.json_format import MessageToJson
from .pageservice.v1 import page_service_pb2, page_service_pb2_grpc

logger = logging.getLogger(__name__)

class DecryptionError(Exception):
    """Raised when page decryption fails."""
    pass

def create_page_service_stub(page_server: str) -> page_service_pb2_grpc.PageServiceStub:
    """Create a gRPC stub for the PageService."""
    channel = grpc.insecure_channel(page_server)
    return page_service_pb2_grpc.PageServiceStub(channel)

def fetch_page(
    stub: page_service_pb2_grpc.PageServiceStub,
    log_id: int,
    table_id: int,
    page_id: int,
    lsn: int,
) -> page_service_pb2.GetPageAtLSNResponse:
    """Fetch a page via the unary GetPageAtLSN RPC."""
    request = page_service_pb2.GetPageAtLSNRequest(
        log_id=log_id,
        table_id=table_id,
        page_id=page_id,
        lsn=lsn,
    )
    return stub.GetPageAtLSN(request)

class DisaggClient:
    def __init__(self, server_addr: str, decryptor_path: str = "pagedecryptor", debug: bool = False):
        self.server_addr = server_addr
        self.decryptor_path = decryptor_path
        self.debug = debug
        self.channel = grpc.insecure_channel(server_addr)
        self.stub = page_service_pb2_grpc.PageServiceStub(self.channel)
        self.test_stub = page_service_pb2_grpc.PageServiceTestServiceStub(self.channel)

    def close(self):
        """Close the underlying gRPC channel."""
        if self.channel is not None:
            self.channel.close()
            self.channel = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def get_page_at_lsn(self, log_id: int, table_id: int, page_id: int, lsn: int):
        logger.debug(
            "gRPC GetPageAtLSN(log_id=%d, table_id=%d, page_id=%d, lsn=%d)",
            log_id, table_id, page_id, lsn,
        )
        request = page_service_pb2.GetPageAtLSNRequest(
            log_id=log_id,
            table_id=table_id,
            page_id=page_id,
            lsn=lsn,
        )
        response = self.stub.GetPageAtLSN(request)
        logger.debug(
            "gRPC GetPageAtLSN response: contents=%d bytes, num_deltas=%d",
            len(response.page.contents), len(response.page.deltas),
        )
        return response

    def get_page_history(self, log_id: int, table_id: int, page_id: int):
        logger.debug(
            "gRPC GetPageHistory(log_id=%d, table_id=%d, page_id=%d)",
            log_id, table_id, page_id,
        )
        request = page_service_pb2.GetPageHistoryRequest(
            log_id=log_id,
            table_id=table_id,
            page_id=page_id,
        )
        response = self.test_stub.GetPageHistory(request)
        logger.debug(
            "gRPC GetPageHistory response: %d metadata entries",
            len(response.metadata),
        )
        return response

    def list_pages(
        self,
        log_id: int,
        table_id: Optional[int] = None,
        page_id: Optional[int] = None,
        lsn: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        logger.debug(
            "gRPC ListPages(log_id=%d, table_id=%s, page_id=%s, lsn=%s, limit=%s)",
            log_id, table_id, page_id, lsn, limit,
        )
        request = page_service_pb2.ListPagesRequest(
            log_id=log_id,
            table_id=table_id,
            page_id=page_id,
            lsn=lsn,
            limit=limit,
        )
        response = self.test_stub.ListPages(request)
        logger.debug(
            "gRPC ListPages response: %d page summaries",
            len(response.page_summaries),
        )
        return response

    def decrypt_full_response(
        self,
        key_file: str,
        response: page_service_pb2.GetPageAtLSNResponse,
        lsn: int,
        table_id: int,
        page_id: int,
    ) -> bytes:
        """Decrypt full response by decrypting base and deltas separately."""
        # Use the standalone function logic to avoid duplication
        return decrypt_full_response_json(
            self.decryptor_path, key_file, response, lsn, table_id, page_id
        )

    def decrypt_bytes(
        self,
        key_file: str,
        encrypted_bytes: bytes,
        lsn: int,
        table_id: int,
        page_id: int,
        *,
        backlink_lsn: Optional[int] = None,
        base_lsn: Optional[int] = None,
        is_delta: bool = False,
    ) -> bytes:
        with tempfile.NamedTemporaryFile(suffix=".out", delete=True) as tmp_out:
            _run_decryptor_bytes_to_file(
                self.decryptor_path, key_file, encrypted_bytes, tmp_out.name,
                lsn, table_id, page_id,
                backlink_lsn=backlink_lsn,
                base_lsn=base_lsn,
                is_delta=is_delta
            )
            return tmp_out.read()

def decrypt_full_response_json(
    decryptor_path: str,
    key_file: str,
    response: page_service_pb2.GetPageAtLSNResponse,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    output_path: Optional[Path] = None,
    debug: bool = False,
) -> bytes:
    """Decrypt page bytes using the pagedecryptor CLI tool by decrypting base and deltas separately."""
    page_proto = response.page
    
    # We need a stable output path for sequential calls
    actual_output_path = output_path
    temp_file_to_cleanup = None
    if not actual_output_path:
        temp_file = tempfile.NamedTemporaryFile(suffix=".out", delete=False)
        actual_output_path = Path(temp_file.name)
        temp_file_to_cleanup = actual_output_path
        temp_file.close()
    else:
        actual_output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Decrypt base image
        # If we have deltas, the 'contents' blob is the full image at full_image_lsn.
        base_lsn = page_proto.full_image_lsn if page_proto.deltas else lsn
        base_backlink = page_proto.full_image_backlink_lsn
        
        _run_decryptor_bytes_to_file(
            decryptor_path, key_file, page_proto.contents, str(actual_output_path),
            base_lsn, table_id, page_id,
            backlink_lsn=base_backlink if base_backlink else None,
            debug=debug
        )

        # Capture the decrypted base image bytes now, before any delta overwrites the file.
        # The decryptor overwrites --outputPath on each call, so reading here gives us the
        # full (non-delta) base image that callers need for page decoding and tree traversal.
        base_bytes = actual_output_path.read_bytes()
        
        # 2. Decrypt deltas sequentially into the same file
        if page_proto.deltas:
            num_deltas = len(page_proto.deltas)
            # Map lsns and backlinks to deltas. Usually lsns[0] is for the base image.
            delta_lsns = list(page_proto.lsns)
            delta_backlinks = list(page_proto.backlinks)
            
            if len(delta_lsns) == num_deltas + 1:
                delta_lsns = delta_lsns[1:]
            if len(delta_backlinks) == num_deltas + 1:
                delta_backlinks = delta_backlinks[1:]

            for i, delta_bytes in enumerate(page_proto.deltas):
                d_lsn = delta_lsns[i] if i < len(delta_lsns) else lsn
                d_backlink = delta_backlinks[i] if i < len(delta_backlinks) else None
                
                _run_decryptor_bytes_to_file(
                    decryptor_path, key_file, delta_bytes, str(actual_output_path),
                    d_lsn, table_id, page_id,
                    backlink_lsn=d_backlink,
                    base_lsn=page_proto.full_image_lsn,
                    is_delta=True,
                    debug=debug
                )
        
        # Return the base image bytes.  When deltas are present the decryptor has overwritten
        # actual_output_path with the last delta; base_bytes holds the stable full-image data
        # that decoders and tree-traversal logic need.  The artifact file at actual_output_path
        # reflects the last decrypted delta (useful for inspecting individual deltas on disk).
        return base_bytes
    finally:
        if temp_file_to_cleanup:
            temp_file_to_cleanup.unlink(missing_ok=True)

def decrypt_response_deltas(
    decryptor_path: str,
    key_file: str,
    response: page_service_pb2.GetPageAtLSNResponse,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    debug: bool = False,
) -> list[tuple[int, bytes]]:
    """Decrypt each delta in the response individually.

    Returns a list of (delta_lsn, decrypted_bytes) pairs in delta order.
    Returns an empty list when the response has no deltas.
    """
    page_proto = response.page
    if not page_proto.deltas:
        return []

    num_deltas = len(page_proto.deltas)
    delta_lsns = list(page_proto.lsns)
    delta_backlinks = list(page_proto.backlinks)

    if len(delta_lsns) == num_deltas + 1:
        delta_lsns = delta_lsns[1:]
    if len(delta_backlinks) == num_deltas + 1:
        delta_backlinks = delta_backlinks[1:]

    results: list[tuple[int, bytes]] = []
    for i, delta_blob in enumerate(page_proto.deltas):
        d_lsn = delta_lsns[i] if i < len(delta_lsns) else lsn
        d_backlink = delta_backlinks[i] if i < len(delta_backlinks) else None

        tmp_file = tempfile.NamedTemporaryFile(suffix=".delta.out", delete=False)
        tmp_path = Path(tmp_file.name)
        tmp_file.close()
        try:
            _run_decryptor_bytes_to_file(
                decryptor_path, key_file, delta_blob, str(tmp_path),
                d_lsn, table_id, page_id,
                backlink_lsn=d_backlink,
                base_lsn=page_proto.full_image_lsn,
                is_delta=True,
                debug=debug,
            )
            results.append((d_lsn, tmp_path.read_bytes()))
        finally:
            tmp_path.unlink(missing_ok=True)

    return results


def _run_decryptor_bytes_to_file(
    decryptor_path: str,
    key_file: str,
    encrypted_bytes: bytes,
    output_path: str,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    backlink_lsn: Optional[int] = None,
    base_lsn: Optional[int] = None,
    is_delta: bool = False,
    debug: bool = False,
) -> None:
    """Helper to run decryptor on a blob of bytes and write/append to a file."""
    b64_data = base64.b64encode(encrypted_bytes).decode("ascii")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".in", delete=True) as tmp_in:
        tmp_in.write(b64_data)
        tmp_in.flush()
        
        cmd = _build_decryptor_cmd(
            decryptor_path, key_file, output_path, lsn, table_id, page_id,
            input_path=tmp_in.name,
            backlink_lsn=backlink_lsn,
            base_lsn=base_lsn,
            is_delta=is_delta
        )

        logger.debug("Running decryptor command: %s", " ".join(cmd))
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if result.stdout:
                logger.debug("Decryptor stdout: %s", result.stdout.strip())
            if result.stderr:
                logger.debug("Decryptor stderr: %s", result.stderr.strip())
        except subprocess.CalledProcessError as e:
            logger.debug("Decryptor failed (lsn=%d, is_delta=%s): %s", lsn, is_delta, e.stderr)
            raise DecryptionError(f"Decryptor failed (lsn={lsn}, is_delta={is_delta}): {e.stderr}") from e

def _build_decryptor_cmd(
    decryptor_path: str,
    key_file: str,
    output_path: str,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    input_path: Optional[str] = None,
    json_page: Optional[str] = None,
    backlink_lsn: Optional[int] = None,
    base_lsn: Optional[int] = None,
    is_delta: bool = False,
) -> List[str]:
    """Helper to build the pagedecryptor command line arguments."""
    cmd = [
        decryptor_path,
        "--outputPath", output_path,
        "--keyFile", key_file,
        "--lsn", str(lsn),
        "--tableId", str(table_id),
        "--pageId", str(page_id),
    ]
    
    if input_path:
        cmd.extend(["--inputPath", input_path])
    if json_page:
        cmd.extend(["--jsonPage", json_page])
        
    if backlink_lsn is not None:
        cmd.extend(["--backlinkLsn", str(backlink_lsn)])
    if base_lsn is not None:
        cmd.extend(["--baseLsn", str(base_lsn)])
    if is_delta:
        cmd.append("--isDelta")
        
    return cmd
