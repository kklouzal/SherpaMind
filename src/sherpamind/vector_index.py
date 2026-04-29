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
DRIFT_SAMPLE_LIMIT = 10


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def _hash_index(token: str, dims: int) -> int:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % dims


def _sparse_vectorize_text(text: str, dims: int = DEFAULT_DIMS) -> dict[int, float]:
    weights: dict[int, float] = {}
    tokens = _tokenize(text)
    if not tokens:
        return weights
    for token in tokens:
        idx = _hash_index(token, dims)
        weights[idx] = weights.get(idx, 0.0) + 1.0
    norm = math.sqrt(sum(v * v for v in weights.values()))
    if norm > 0:
        weights = {idx: value / norm for idx, value in weights.items()}
    return weights


def vectorize_text(text: str, dims: int = DEFAULT_DIMS) -> list[float]:
    vec = [0.0] * dims
    for idx, weight in _sparse_vectorize_text(text, dims=dims).items():
        vec[idx] = weight
    return vec


def _cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _load_chunk_rows(
    db_path: Path,
    limit: int | None = None,
    *,
    ticket_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    query = """
        SELECT chunk_id, doc_id, ticket_id, text, content_hash
        FROM ticket_document_chunks
    """
    params: list[Any] = []
    if ticket_ids:
        placeholders = ",".join("?" for _ in ticket_ids)
        query += f" WHERE ticket_id IN ({placeholders})"
        params.extend(ticket_ids)
    query += " ORDER BY ticket_id DESC, chunk_index ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def build_vector_index(
    db_path: Path,
    dims: int = DEFAULT_DIMS,
    limit: int | None = None,
    *,
    ticket_ids: list[str] | None = None,
) -> dict[str, Any]:
    initialize_db(db_path)
    scoped_ticket_ids = sorted({str(ticket_id) for ticket_id in (ticket_ids or []) if str(ticket_id).strip()})
    rows = _load_chunk_rows(db_path, limit=limit, ticket_ids=scoped_ticket_ids or None)
    current_ids = {row["chunk_id"] for row in rows}
    inserted_or_updated = 0
    skipped_unchanged = 0
    with connect(db_path) as conn:
        if scoped_ticket_ids:
            placeholders = ",".join("?" for _ in scoped_ticket_ids)
            existing_rows = conn.execute(
                f"""
                SELECT v.chunk_id, v.content_hash, v.dims, COUNT(vt.dim) AS term_count
                FROM vector_chunk_index v
                LEFT JOIN vector_chunk_terms vt ON vt.chunk_id = v.chunk_id
                WHERE v.ticket_id IN ({placeholders})
                GROUP BY v.chunk_id, v.content_hash, v.dims
                """,
                tuple(scoped_ticket_ids),
            ).fetchall()
        else:
            existing_rows = conn.execute(
                """
                SELECT v.chunk_id, v.content_hash, v.dims, COUNT(vt.dim) AS term_count
                FROM vector_chunk_index v
                LEFT JOIN vector_chunk_terms vt ON vt.chunk_id = v.chunk_id
                GROUP BY v.chunk_id, v.content_hash, v.dims
                """
            ).fetchall()
        existing = {row["chunk_id"]: dict(row) for row in existing_rows}
        synced_at = now_iso()
        for row in rows:
            current = existing.get(row["chunk_id"])
            is_current = current and current.get("content_hash") == row.get("content_hash") and int(current.get("dims") or 0) == dims
            if is_current and int(current.get("term_count") or 0) > 0:
                skipped_unchanged += 1
                continue
            sparse_vector = _sparse_vectorize_text(row["text"], dims=dims)
            vector = [0.0] * dims
            for idx, weight in sparse_vector.items():
                vector[idx] = weight
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
                    row.get("content_hash"),
                    synced_at,
                ),
            )
            conn.execute("DELETE FROM vector_chunk_terms WHERE chunk_id = ?", (row["chunk_id"],))
            conn.executemany(
                """
                INSERT INTO vector_chunk_terms(chunk_id, doc_id, ticket_id, dim, weight, synced_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                [(row["chunk_id"], row["doc_id"], row["ticket_id"], idx, weight, synced_at) for idx, weight in sparse_vector.items()],
            )
            inserted_or_updated += 1
        stale_ids = [chunk_id for chunk_id in existing if chunk_id not in current_ids]
        if stale_ids:
            placeholders = ",".join("?" for _ in stale_ids)
            conn.execute(f"DELETE FROM vector_chunk_terms WHERE chunk_id IN ({placeholders})", stale_ids)
            conn.execute(f"DELETE FROM vector_chunk_index WHERE chunk_id IN ({placeholders})", stale_ids)
        conn.commit()
    return {
        "status": "ok",
        "indexed_chunks": len(rows),
        "inserted_or_updated": inserted_or_updated,
        "skipped_unchanged": skipped_unchanged,
        "deleted_stale": len(stale_ids),
        "dims": dims,
        "ticket_scope_count": len(scoped_ticket_ids) if scoped_ticket_ids else None,
    }


def _vector_drift_samples(conn: Any, *, limit: int = DRIFT_SAMPLE_LIMIT) -> dict[str, list[dict[str, Any]]]:
    missing_chunks = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                c.chunk_id,
                c.doc_id,
                c.ticket_id,
                c.chunk_index,
                c.synced_at AS chunk_synced_at,
                d.status,
                d.account,
                d.technician,
                d.updated_at
            FROM ticket_document_chunks c
            LEFT JOIN vector_chunk_index v ON v.chunk_id = c.chunk_id
            LEFT JOIN ticket_documents d ON d.doc_id = c.doc_id
            WHERE v.chunk_id IS NULL
            ORDER BY d.updated_at DESC, c.ticket_id DESC, c.chunk_index ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    outdated_chunks = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                c.chunk_id,
                c.doc_id,
                c.ticket_id,
                c.chunk_index,
                c.synced_at AS chunk_synced_at,
                v.synced_at AS index_synced_at,
                c.content_hash AS chunk_content_hash,
                v.content_hash AS index_content_hash,
                d.status,
                d.account,
                d.technician,
                d.updated_at
            FROM ticket_document_chunks c
            JOIN vector_chunk_index v ON v.chunk_id = c.chunk_id
            LEFT JOIN ticket_documents d ON d.doc_id = c.doc_id
            WHERE COALESCE(v.content_hash, '') != COALESCE(c.content_hash, '')
            ORDER BY d.updated_at DESC, c.ticket_id DESC, c.chunk_index ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    dangling_index_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                v.chunk_id,
                v.doc_id,
                v.ticket_id,
                v.synced_at AS index_synced_at,
                v.content_hash AS index_content_hash,
                v.dims
            FROM vector_chunk_index v
            LEFT JOIN ticket_document_chunks c ON c.chunk_id = v.chunk_id
            WHERE c.chunk_id IS NULL
            ORDER BY v.synced_at DESC, v.ticket_id DESC, v.chunk_id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    missing_documents = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                c.doc_id,
                c.ticket_id,
                COUNT(*) AS missing_chunks,
                d.status,
                d.account,
                d.technician,
                d.updated_at
            FROM ticket_document_chunks c
            LEFT JOIN vector_chunk_index v ON v.chunk_id = c.chunk_id
            LEFT JOIN ticket_documents d ON d.doc_id = c.doc_id
            WHERE v.chunk_id IS NULL
            GROUP BY c.doc_id, c.ticket_id, d.status, d.account, d.technician, d.updated_at
            ORDER BY missing_chunks DESC, d.updated_at DESC, c.ticket_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    outdated_documents = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                c.doc_id,
                c.ticket_id,
                COUNT(*) AS outdated_chunks,
                d.status,
                d.account,
                d.technician,
                d.updated_at
            FROM ticket_document_chunks c
            JOIN vector_chunk_index v ON v.chunk_id = c.chunk_id
            LEFT JOIN ticket_documents d ON d.doc_id = c.doc_id
            WHERE COALESCE(v.content_hash, '') != COALESCE(c.content_hash, '')
            GROUP BY c.doc_id, c.ticket_id, d.status, d.account, d.technician, d.updated_at
            ORDER BY outdated_chunks DESC, d.updated_at DESC, c.ticket_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    return {
        "missing_chunks": missing_chunks,
        "outdated_chunks": outdated_chunks,
        "dangling_index_rows": dangling_index_rows,
        "missing_documents": missing_documents,
        "outdated_documents": outdated_documents,
    }


def get_vector_index_status(db_path: Path) -> dict[str, Any]:
    initialize_db(db_path)
    with connect(db_path) as conn:
        totals = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM ticket_document_chunks) AS total_chunk_rows,
                (SELECT COUNT(*) FROM vector_chunk_index) AS indexed_chunks,
                (SELECT COUNT(*) FROM vector_chunk_terms) AS indexed_terms,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT v.chunk_id
                        FROM vector_chunk_index v
                        LEFT JOIN vector_chunk_terms vt ON vt.chunk_id = v.chunk_id
                        GROUP BY v.chunk_id
                        HAVING COUNT(vt.dim) = 0
                    ) missing_terms
                ) AS missing_term_rows,
                (SELECT MIN(synced_at) FROM vector_chunk_index) AS earliest_sync_at,
                (SELECT MAX(synced_at) FROM vector_chunk_index) AS latest_sync_at,
                (SELECT COUNT(DISTINCT dims) FROM vector_chunk_index) AS distinct_dims,
                (SELECT MIN(dims) FROM vector_chunk_index) AS min_dims,
                (SELECT MAX(dims) FROM vector_chunk_index) AS max_dims,
                (
                    SELECT COUNT(*)
                    FROM vector_chunk_index v
                    LEFT JOIN ticket_document_chunks c ON c.chunk_id = v.chunk_id
                    WHERE c.chunk_id IS NULL
                ) AS dangling_index_rows,
                (
                    SELECT COUNT(*)
                    FROM ticket_document_chunks c
                    LEFT JOIN vector_chunk_index v ON v.chunk_id = c.chunk_id
                    WHERE v.chunk_id IS NULL
                ) AS missing_index_rows,
                (
                    SELECT COUNT(*)
                    FROM vector_chunk_index v
                    JOIN ticket_document_chunks c ON c.chunk_id = v.chunk_id
                    WHERE COALESCE(v.content_hash, '') != COALESCE(c.content_hash, '')
                ) AS outdated_content_rows
            """
        ).fetchone()
        drift_samples = _vector_drift_samples(conn)
    indexed_chunks = int(totals["indexed_chunks"] or 0)
    total_chunk_rows = int(totals["total_chunk_rows"] or 0)
    ready_ratio = round(indexed_chunks / total_chunk_rows, 6) if total_chunk_rows else 0.0
    return {
        "indexed_chunks": indexed_chunks,
        "indexed_terms": int(totals["indexed_terms"] or 0),
        "missing_term_rows": int(totals["missing_term_rows"] or 0),
        "total_chunk_rows": total_chunk_rows,
        "ready_ratio": ready_ratio,
        "earliest_sync_at": totals["earliest_sync_at"],
        "latest_sync_at": totals["latest_sync_at"],
        "distinct_dims": int(totals["distinct_dims"] or 0),
        "min_dims": totals["min_dims"],
        "max_dims": totals["max_dims"],
        "dangling_index_rows": int(totals["dangling_index_rows"] or 0),
        "missing_index_rows": int(totals["missing_index_rows"] or 0),
        "outdated_content_rows": int(totals["outdated_content_rows"] or 0),
        "drift_sample_limit": DRIFT_SAMPLE_LIMIT,
        "drift_samples": drift_samples,
    }


def ensure_current_vector_index(db_path: Path, dims: int = DEFAULT_DIMS) -> dict[str, Any]:
    status = get_vector_index_status(db_path)
    needs_refresh = bool(
        status["missing_index_rows"]
        or status["missing_term_rows"]
        or status["dangling_index_rows"]
        or status["outdated_content_rows"]
        or (status["indexed_chunks"] and (status["distinct_dims"] != 1 or status["min_dims"] != dims or status["max_dims"] != dims))
    )
    if not needs_refresh:
        return {"status": "ok", "refreshed": False, "vector_index": status}
    refreshed = build_vector_index(db_path, dims=dims)
    refreshed_status = get_vector_index_status(db_path)
    return {"status": "ok", "refreshed": True, "reason": {"missing_index_rows": status["missing_index_rows"], "missing_term_rows": status["missing_term_rows"], "dangling_index_rows": status["dangling_index_rows"], "outdated_content_rows": status["outdated_content_rows"]}, "vector_index": refreshed_status, "refresh_result": refreshed}


def search_vector_index(
    db_path: Path,
    query_text: str,
    limit: int = 10,
    account: str | None = None,
    status: str | None = None,
    technician: str | None = None,
    priority: str | None = None,
    category: str | None = None,
    class_name: str | None = None,
    submission_category: str | None = None,
    resolution_category: str | None = None,
    department: str | None = None,
) -> list[dict[str, Any]]:
    initialize_db(db_path)
    with connect(db_path) as conn:
        dim_row = conn.execute("SELECT dims FROM vector_chunk_index LIMIT 1").fetchone()
        if not dim_row:
            return []
        dims = int(dim_row["dims"])
        query_sparse = _sparse_vectorize_text(query_text, dims=dims)
        if not query_sparse:
            return []
        term_count = conn.execute("SELECT COUNT(*) AS c FROM vector_chunk_terms").fetchone()["c"]
        indexed_count = conn.execute("SELECT COUNT(*) AS c FROM vector_chunk_index").fetchone()["c"]
    if indexed_count and not term_count:
        build_vector_index(db_path, dims=dims)

    values_sql = ", ".join("(?, ?)" for _ in query_sparse)
    query_params: list[Any] = []
    for dim, weight in query_sparse.items():
        query_params.extend([dim, weight])

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
    if priority:
        clauses.append("json_extract(d.raw_json, '$.metadata.priority') = ?")
        params.append(priority)
    if category:
        clauses.append("json_extract(d.raw_json, '$.metadata.category') LIKE ? COLLATE NOCASE")
        params.append(f"%{category}%")
    if class_name:
        clauses.append("json_extract(d.raw_json, '$.metadata.class_name') LIKE ? COLLATE NOCASE")
        params.append(f"%{class_name}%")
    if submission_category:
        clauses.append("json_extract(d.raw_json, '$.metadata.submission_category') LIKE ? COLLATE NOCASE")
        params.append(f"%{submission_category}%")
    if resolution_category:
        clauses.append("json_extract(d.raw_json, '$.metadata.resolution_category') LIKE ? COLLATE NOCASE")
        params.append(f"%{resolution_category}%")
    if department:
        clauses.append("json_extract(d.raw_json, '$.metadata.department_label') LIKE ? COLLATE NOCASE")
        params.append(f"%{department}%")
    all_clauses = ["candidates.score > 0", *clauses]
    where = f"WHERE {' AND '.join(all_clauses)}"

    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            WITH query_terms(dim, q_weight) AS (VALUES {values_sql}),
            candidates AS (
                SELECT
                    vt.chunk_id,
                    SUM(q.q_weight * vt.weight) AS score
                FROM query_terms q
                JOIN vector_chunk_terms vt ON vt.dim = q.dim
                GROUP BY vt.chunk_id
            )
            SELECT v.chunk_id, v.doc_id, v.ticket_id, v.content_hash,
                   c.chunk_index, c.text,
                   d.account, d.status, d.technician, d.updated_at,
                   json_extract(d.raw_json, '$.metadata.priority') AS priority,
                   json_extract(d.raw_json, '$.metadata.category') AS category,
                   json_extract(d.raw_json, '$.metadata.class_name') AS class_name,
                   json_extract(d.raw_json, '$.metadata.submission_category') AS submission_category,
                   json_extract(d.raw_json, '$.metadata.resolution_category') AS resolution_category,
                   json_extract(d.raw_json, '$.metadata.department_label') AS department_label,
                   candidates.score AS score
            FROM candidates
            JOIN vector_chunk_index v ON v.chunk_id = candidates.chunk_id
            JOIN ticket_document_chunks c ON c.chunk_id = v.chunk_id
            JOIN ticket_documents d ON d.doc_id = v.doc_id
            {where}
            ORDER BY candidates.score DESC, v.ticket_id ASC, c.chunk_index ASC
            LIMIT ?
            """,
            tuple(query_params + params + [limit]),
        ).fetchall()
    return [
        {
            "chunk_id": row["chunk_id"],
            "doc_id": row["doc_id"],
            "ticket_id": row["ticket_id"],
            "chunk_index": row["chunk_index"],
            "account": row["account"],
            "status": row["status"],
            "technician": row["technician"],
            "priority": row["priority"],
            "category": row["category"],
            "class_name": row["class_name"],
            "submission_category": row["submission_category"],
            "resolution_category": row["resolution_category"],
            "department_label": row["department_label"],
            "updated_at": row["updated_at"],
            "content_hash": row["content_hash"],
            "score": round(float(row["score"] or 0.0), 6),
            "text": row["text"],
        }
        for row in rows
    ]
