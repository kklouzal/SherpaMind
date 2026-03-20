from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

from .db import connect, initialize_db, now_iso

TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
DEFAULT_DIMS = 256


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def _hash_index(token: str, dims: int) -> int:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % dims


def vectorize_text(text: str, dims: int = DEFAULT_DIMS) -> list[float]:
    vec = [0.0] * dims
    tokens = _tokenize(text)
    if not tokens:
        return vec
    for token in tokens:
        vec[_hash_index(token, dims)] += 1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def _cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def build_vector_index(db_path: Path, dims: int = DEFAULT_DIMS, limit: int | None = None) -> dict[str, Any]:
    initialize_db(db_path)
    query = """
        SELECT chunk_id, doc_id, ticket_id, text, content_hash
        FROM ticket_document_chunks
        ORDER BY ticket_id DESC, chunk_index ASC
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)
    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
        synced_at = now_iso()
        for row in rows:
            vector = vectorize_text(row["text"], dims=dims)
            conn.execute(
                """
                INSERT INTO vector_chunk_index(chunk_id, doc_id, ticket_id, vector_json, dims, content_hash, synced_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chunk_id) DO UPDATE SET
                    doc_id = excluded.doc_id,
                    ticket_id = excluded.ticket_id,
                    vector_json = excluded.vector_json,
                    dims = excluded.dims,
                    content_hash = excluded.content_hash,
                    synced_at = excluded.synced_at
                """,
                (
                    row["chunk_id"],
                    row["doc_id"],
                    row["ticket_id"],
                    json.dumps(vector),
                    dims,
                    row["content_hash"],
                    synced_at,
                ),
            )
        conn.commit()
    return {"status": "ok", "indexed_chunks": len(rows), "dims": dims}


def search_vector_index(
    db_path: Path,
    query_text: str,
    limit: int = 10,
    account: str | None = None,
    status: str | None = None,
    technician: str | None = None,
) -> list[dict[str, Any]]:
    initialize_db(db_path)
    with connect(db_path) as conn:
        dim_row = conn.execute("SELECT dims FROM vector_chunk_index LIMIT 1").fetchone()
        if not dim_row:
            return []
        dims = int(dim_row["dims"])
        query_vec = vectorize_text(query_text, dims=dims)
        clauses = []
        params: list[Any] = []
        if account:
            clauses.append("d.account LIKE ? COLLATE NOCASE")
            params.append(f"%{account}%")
        if status:
            clauses.append("d.status = ?")
            params.append(status)
        if technician:
            clauses.append("d.technician LIKE ? COLLATE NOCASE")
            params.append(f"%{technician}%")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"""
            SELECT v.chunk_id, v.doc_id, v.ticket_id, v.vector_json, v.content_hash,
                   c.chunk_index, c.text,
                   d.account, d.status, d.technician, d.updated_at
            FROM vector_chunk_index v
            JOIN ticket_document_chunks c ON c.chunk_id = v.chunk_id
            JOIN ticket_documents d ON d.doc_id = v.doc_id
            {where}
            """,
            tuple(params),
        ).fetchall()
    scored = []
    for row in rows:
        vector = json.loads(row["vector_json"])
        score = _cosine(query_vec, vector)
        if score <= 0:
            continue
        scored.append(
            {
                "chunk_id": row["chunk_id"],
                "doc_id": row["doc_id"],
                "ticket_id": row["ticket_id"],
                "chunk_index": row["chunk_index"],
                "account": row["account"],
                "status": row["status"],
                "technician": row["technician"],
                "updated_at": row["updated_at"],
                "content_hash": row["content_hash"],
                "score": round(score, 6),
                "text": row["text"],
            }
        )
    scored.sort(key=lambda item: (-item["score"], item["ticket_id"], item["chunk_index"]))
    return scored[:limit]
