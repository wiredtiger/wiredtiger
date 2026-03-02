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
    def __init__(self, server_addr: str, decryptor_path: str = "pagedecryptor"):
        self.server_addr = server_addr
        self.decryptor_path = decryptor_path
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
        request = page_service_pb2.GetPageAtLSNRequest(
            log_id=log_id,
            table_id=table_id,
            page_id=page_id,
            lsn=lsn,
        )
        return self.stub.GetPageAtLSN(request)

    def get_page_history(self, log_id: int, table_id: int, page_id: int):
        request = page_service_pb2.GetPageHistoryRequest(
            log_id=log_id,
            table_id=table_id,
            page_id=page_id,
        )
        return self.test_stub.GetPageHistory(request)

    def decrypt_full_response(
        self,
        key_file: str,
        response: page_service_pb2.GetPageAtLSNResponse,
        lsn: int,
        table_id: int,
        page_id: int,
    ) -> bytes:
        page_proto = response.page
        # Newer pages (especially root/turtle pages) may require the backlink for decryption.
        backlink = page_proto.full_image_backlink_lsn
        
        if not page_proto.deltas:
             return self.decrypt_bytes(
                key_file,
                page_proto.contents,
                lsn, table_id, page_id,
                backlink_lsn=backlink if backlink else None,
            )
        else:
            return self.decrypt_bytes(
                key_file,
                page_proto.contents,
                page_proto.full_image_lsn, table_id, page_id,
                backlink_lsn=backlink if backlink else None,
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
        b64_data = base64.b64encode(encrypted_bytes).decode("ascii")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".in", delete=True) as tmp_in, \
             tempfile.NamedTemporaryFile(mode="rb", suffix=".out", delete=True) as tmp_out:
            tmp_in.write(b64_data)
            tmp_in.flush()
            
            cmd = _build_decryptor_cmd(
                self.decryptor_path, key_file, tmp_out.name, lsn, table_id, page_id,
                input_path=tmp_in.name,
                backlink_lsn=backlink_lsn,
                base_lsn=base_lsn,
                is_delta=is_delta
            )

            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                return tmp_out.read()
            except subprocess.CalledProcessError as e:
                raise DecryptionError(f"Decryptor failed: {e.stderr}") from e

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
    """Decrypt page bytes using the pagedecryptor CLI tool with --jsonPage."""
    json_str = MessageToJson(response)
    backlink = response.page.full_image_backlink_lsn
    
    out_file = str(output_path) if output_path else None

    # If no output path provided, use a temp file
    if not out_file:
        with tempfile.NamedTemporaryFile(mode="rb", suffix=".out", delete=True) as tmp_out:
            _run_decryptor_json(
                decryptor_path, key_file, json_str, tmp_out.name,
                lsn, table_id, page_id,
                backlink_lsn=backlink if backlink else None,
                debug=debug,
            )
            return tmp_out.read()
    else:
        # Ensure parent exists
        if output_path:
             output_path.parent.mkdir(parents=True, exist_ok=True)
             
        _run_decryptor_json(
            decryptor_path, key_file, json_str, out_file,
            lsn, table_id, page_id, 
            backlink_lsn=backlink if backlink else None,
            debug=debug,
        )
        if output_path:
            return output_path.read_bytes()
        return b"" # Should not happen based on logic above

def _run_decryptor_json(
    decryptor_path: str,
    key_file: str,
    json_str: str,
    output_path: str,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    backlink_lsn: Optional[int] = None,
    debug: bool = False,
) -> None:
    cmd = _build_decryptor_cmd(
        decryptor_path, key_file, output_path, lsn, table_id, page_id,
        json_page=json_str,
        backlink_lsn=backlink_lsn
    )
    
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise DecryptionError(f"Decryptor (JSON) failed: {e.stderr}") from e

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
