"""分年生成农机城市候选接续边，并输出完整质量审计。

正式口径以项目 PLAN.md 与 DECISIONS.md 为准。该模块不修改原始数据，
也不把缺失值填为 0；所有正式输出在全部年份通过检查后才从临时目录移入。
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb


WRITE_DATE = "2026-10-16"
DECAY_BASE = 0.5
EXPECTED_COLUMNS = {
    "info_id",
    "id",
    "did",
    "date",
    "start_time",
    "end_time",
    "center_time",
    "center_location",
    "tag",
}
YEAR_CONFIG = {
    2021: {
        "input": "Data/00_原始数据_只读/final_filtered_21_modified.csv",
        "output": "Data/03_有向边数据/2021年农机城市接续.csv",
        "expected_rows": 770_845,
        "start_date": "2021-05-01",
        "end_date": "2021-06-25",
    },
    2022: {
        "input": "Data/00_原始数据_只读/final_filtered_22_modified.csv",
        "output": "Data/03_有向边数据/2022年农机城市接续.csv",
        "expected_rows": 2_475_805,
        "start_date": "2022-05-01",
        "end_date": "2022-06-25",
    },
    2023: {
        "input": "Data/00_原始数据_只读/final_filtered_23_modified.csv",
        "output": "Data/03_有向边数据/2023年农机城市接续.csv",
        "expected_rows": 3_976_426,
        "start_date": "2023-05-01",
        "end_date": "2023-06-25",
    },
}
AUDIT_RELATIVE_PATH = "Results/00_数据审计/城市接续审计.csv"
SORT_SQL = (
    "date_value, start_ts, end_ts, center_ts NULLS LAST, "
    "info_id NULLS LAST, id NULLS LAST, source_row_no"
)
OUTPUT_COLUMNS = [
    "边记录编号",
    "年份",
    "农机编号",
    "链段编号",
    "起点访问序号",
    "终点访问序号",
    "起点城市",
    "终点城市",
    "边级",
    "跨越城市数",
    "衰减权重",
    "起点进入时间",
    "起点离开时间",
    "终点进入时间",
    "终点离开时间",
    "时间间隔小时",
    "起点原始记录数",
    "终点原始记录数",
    "起点首行号",
    "起点末行号",
    "终点首行号",
    "终点末行号",
    "起点是否回访",
    "终点是否回访",
    "终点城市访问序次",
    "是否直接回流边",
    "是否同城端点",
    "是否时间冲突",
    "是否同起点时间",
    "来源文件",
]


def _now_text() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return next(csv.reader(handle))


def _scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return connection.execute(sql).fetchone()[0]


def _assert_zero(connection: duckdb.DuckDBPyConnection, sql: str, message: str) -> None:
    value = int(_scalar(connection, sql) or 0)
    if value != 0:
        raise RuntimeError(f"{message}（发现 {value:,} 条）")


def _copy_with_bom(source: Path, destination: Path) -> None:
    with source.open("rb") as src, destination.open("wb") as dst:
        dst.write(b"\xef\xbb\xbf")
        shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)


def _audit_row(
    rows: list[dict[str, Any]],
    run_id: str,
    year: int | str,
    group: str,
    metric: str,
    value: Any,
    unit: str,
    status: str,
    note: str,
    input_file: str,
    output_file: str,
) -> None:
    rows.append(
        {
            "运行编号": run_id,
            "运行时间": _now_text(),
            "年份": year,
            "指标组": group,
            "指标": metric,
            "数值": value,
            "单位": unit,
            "检查状态": status,
            "说明": note,
            "输入文件": input_file,
            "输出文件": output_file,
        }
    )


def _sequence_edges(sequence: list[str | None]) -> list[tuple[str, str, int, float]]:
    """仅供小样断言；None 代表缺失城市断点。"""
    chains: list[list[str]] = []
    current: list[str] = []
    for city in sequence:
        if city is None:
            if current:
                chains.append(current)
            current = []
            continue
        if not current or current[-1] != city:
            current.append(city)
    if current:
        chains.append(current)
    result: list[tuple[str, str, int, float]] = []
    for chain in chains:
        for i in range(len(chain)):
            for j in range(i + 1, len(chain)):
                level = j - i
                result.append((chain[i], chain[j], level, DECAY_BASE ** (level - 1)))
    return result


def run_unit_tests() -> None:
    """验证边级、权重、连续同城合并、断链和回访保留。"""
    abcd = _sequence_edges(["A", "B", "C", "D"])
    assert len(abcd) == 6 and abs(sum(x[3] for x in abcd) - 4.25) < 1e-15
    aaba = _sequence_edges(["A", "A", "B", "A", "A"])
    assert len(aaba) == 3 and ("A", "A", 2, 0.5) in aaba
    broken = _sequence_edges(["A", "B", None, "C", "D"])
    assert broken == [("A", "B", 1, 1.0), ("C", "D", 1, 1.0)]
    return_path = _sequence_edges(["A", "B", "C", "D", "C", "E"])
    assert len(return_path) == 15
    assert abs(sum(x[3] for x in return_path) - 8.0625) < 1e-15
    repeated_pairs = _sequence_edges(["A", "B", "A", "B"])
    assert len(repeated_pairs) == 6


def inspect_inputs(project_root: Path) -> list[dict[str, Any]]:
    """只读检查输入路径、表头、大小和预期输出位置。"""
    project_root = project_root.resolve()
    report: list[dict[str, Any]] = []
    for year, config in YEAR_CONFIG.items():
        input_path = project_root / config["input"]
        output_path = project_root / config["output"]
        if not input_path.is_file():
            raise FileNotFoundError(f"缺少正式输入：{input_path}")
        header = _read_header(input_path)
        missing = sorted(EXPECTED_COLUMNS.difference(header))
        if missing:
            raise RuntimeError(f"{year} 年输入缺少字段：{missing}")
        report.append(
            {
                "year": year,
                "input": str(input_path),
                "input_bytes": input_path.stat().st_size,
                "column_count": len(header),
                "output": str(output_path),
            }
        )
    return report


def _prepare_overwrite_backup(project_root: Path, paths: list[Path]) -> str:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return ""
    timestamp = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d_%H%M%S")
    backup_dir = (project_root / "Archive" / f"{timestamp}_城市接续重跑前").resolve()
    if project_root.resolve() not in backup_dir.parents:
        raise RuntimeError("备份目录越出项目范围，已停止。")
    backup_dir.mkdir(parents=True, exist_ok=False)
    for path in existing:
        relative = path.resolve().relative_to(project_root.resolve())
        safe_name = "__".join(relative.parts)
        shutil.copy2(path, backup_dir / safe_name)
    return str(backup_dir.relative_to(project_root)).replace("\\", "/")


def _process_year(
    project_root: Path,
    temp_dir: Path,
    year: int,
    config: dict[str, Any],
    run_id: str,
    audit_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    input_path = (project_root / config["input"]).resolve()
    output_path = (project_root / config["output"]).resolve()
    input_rel = config["input"]
    output_rel = config["output"]
    database_path = temp_dir / f"{year}.duckdb"
    csv_without_bom = temp_dir / f"{year}_utf8.csv"
    csv_with_bom = temp_dir / output_path.name
    connection = duckdb.connect(str(database_path))
    connection.execute(f"PRAGMA threads={max(1, min(os.cpu_count() or 1, 8))}")
    connection.execute("PRAGMA preserve_insertion_order=true")
    try:
        connection.execute(
            f"""
            CREATE TABLE raw AS
            SELECT
                row_number() OVER ()::BIGINT AS source_row_no,
                trim(info_id) AS info_id,
                trim(id) AS id,
                trim(did) AS did,
                trim(date) AS date_text,
                trim(start_time) AS start_time_text,
                trim(end_time) AS end_time_text,
                trim(center_time) AS center_time_text,
                trim(center_location) AS center_location_text,
                trim(tag) AS tag_text
            FROM read_csv_auto(
                '{_sql_path(input_path)}',
                header=true,
                all_varchar=true,
                parallel=false,
                ignore_errors=false
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE parsed AS
            SELECT
                source_row_no,
                NULLIF(info_id, '') AS info_id,
                NULLIF(id, '') AS id,
                NULLIF(did, '') AS did,
                NULLIF(date_text, '') AS date_text,
                try_cast(NULLIF(date_text, '') AS DATE) AS date_value,
                NULLIF(start_time_text, '') AS start_time_text,
                try_cast(NULLIF(start_time_text, '') AS TIMESTAMP) AS start_ts,
                NULLIF(end_time_text, '') AS end_time_text,
                try_cast(NULLIF(end_time_text, '') AS TIMESTAMP) AS end_ts,
                NULLIF(center_time_text, '') AS center_time_text,
                coalesce(
                    try_strptime(NULLIF(center_time_text, ''), '%Y%m%d%H%M%S'),
                    try_cast(NULLIF(center_time_text, '') AS TIMESTAMP)
                ) AS center_ts,
                NULLIF(center_location_text, '') AS city,
                tag_text
            FROM raw
            """
        )

        input_rows = int(_scalar(connection, "SELECT count(*) FROM parsed"))
        unique_did = int(_scalar(connection, "SELECT count(DISTINCT did) FROM parsed"))
        city_count = int(_scalar(connection, "SELECT count(DISTINCT city) FROM parsed"))
        date_min, date_max = connection.execute(
            "SELECT min(date_value)::VARCHAR, max(date_value)::VARCHAR FROM parsed"
        ).fetchone()
        critical_counts = connection.execute(
            """
            SELECT
                count(*) FILTER (WHERE did IS NULL),
                count(*) FILTER (WHERE date_text IS NULL OR date_value IS NULL),
                count(*) FILTER (WHERE start_time_text IS NULL OR start_ts IS NULL),
                count(*) FILTER (WHERE end_time_text IS NULL OR end_ts IS NULL),
                count(*) FILTER (WHERE city IS NULL),
                count(*) FILTER (WHERE start_ts IS NOT NULL AND end_ts IS NOT NULL AND end_ts < start_ts),
                count(*) FILTER (WHERE tag_text IS DISTINCT FROM '1'),
                count(*) FILTER (WHERE date_value IS NOT NULL AND start_ts IS NOT NULL
                                  AND date_value IS DISTINCT FROM cast(start_ts AS DATE))
            FROM parsed
            """
        ).fetchone()
        (
            missing_did,
            invalid_date,
            invalid_start,
            invalid_end,
            missing_city,
            reversed_time,
            non_field_tag,
            date_time_mismatch,
        ) = map(int, critical_counts)

        if input_rows != config["expected_rows"]:
            raise RuntimeError(
                f"{year} 年输入行数为 {input_rows:,}，与正式字典 {config['expected_rows']:,} 不一致。"
            )
        if date_min != config["start_date"] or date_max != config["end_date"]:
            raise RuntimeError(
                f"{year} 年日期范围为 {date_min}—{date_max}，与正式范围不一致。"
            )
        unplaceable = missing_did + invalid_date + invalid_start + invalid_end + reversed_time
        if unplaceable:
            raise RuntimeError(
                f"{year} 年有 {unplaceable:,} 条记录无法可靠定位或排序："
                f"did缺失={missing_did}, 日期无效={invalid_date}, 开始时间无效={invalid_start}, "
                f"结束时间无效={invalid_end}, 结束早于开始={reversed_time}。已停止，未写正式输出。"
            )
        if non_field_tag:
            raise RuntimeError(f"{year} 年存在 {non_field_tag:,} 条 tag 不为 1 的记录。")

        same_start_diff_city_groups = int(
            _scalar(
                connection,
                """
                SELECT count(*) FROM (
                    SELECT did, start_ts
                    FROM parsed
                    GROUP BY did, start_ts
                    HAVING count(DISTINCT city) > 1
                )
                """,
            )
        )
        exact_sort_tie_groups = int(
            _scalar(
                connection,
                """
                SELECT count(*) FROM (
                    SELECT did, date_value, start_ts, end_ts, center_ts, info_id, id
                    FROM parsed
                    GROUP BY ALL
                    HAVING count(*) > 1
                )
                """,
            )
        )

        connection.execute(
            f"""
            CREATE TABLE ordered_flags AS
            SELECT *,
                city IS NULL AS city_missing,
                coalesce(
                    lag(city IS NULL) OVER (PARTITION BY did ORDER BY {SORT_SQL}),
                    false
                ) AS previous_city_missing
            FROM parsed
            """
        )
        connection.execute(
            f"""
            CREATE TABLE ordered_groups AS
            SELECT *,
                CASE WHEN city_missing AND NOT previous_city_missing THEN 1 ELSE 0 END AS break_start,
                sum(CASE WHEN city_missing AND NOT previous_city_missing THEN 1 ELSE 0 END)
                    OVER (PARTITION BY did ORDER BY {SORT_SQL} ROWS UNBOUNDED PRECEDING)
                    AS raw_break_group
            FROM ordered_flags
            """
        )
        missing_city_blocks = int(_scalar(connection, "SELECT sum(break_start)::BIGINT FROM ordered_groups"))
        connection.execute(
            """
            CREATE TABLE valid_records AS
            SELECT *,
                dense_rank() OVER (PARTITION BY did ORDER BY raw_break_group)::INTEGER AS chain_segment
            FROM ordered_groups
            WHERE NOT city_missing
            """
        )
        connection.execute(
            f"""
            CREATE TABLE visit_flags AS
            SELECT *,
                CASE
                    WHEN lag(city) OVER (
                        PARTITION BY did, chain_segment ORDER BY {SORT_SQL}
                    ) IS NULL THEN 1
                    WHEN city IS DISTINCT FROM lag(city) OVER (
                        PARTITION BY did, chain_segment ORDER BY {SORT_SQL}
                    ) THEN 1
                    ELSE 0
                END AS new_visit
            FROM valid_records
            """
        )
        connection.execute(
            f"""
            CREATE TABLE visit_rows AS
            SELECT *,
                sum(new_visit) OVER (
                    PARTITION BY did, chain_segment
                    ORDER BY {SORT_SQL} ROWS UNBOUNDED PRECEDING
                )::BIGINT AS visit_group
            FROM visit_flags
            """
        )
        connection.execute(
            f"""
            CREATE TABLE visits_base AS
            SELECT
                {year}::INTEGER AS year,
                did,
                chain_segment,
                visit_group,
                first(city ORDER BY {SORT_SQL}) AS city,
                min(start_ts) AS entry_time,
                max(end_ts) AS exit_time,
                count(*)::BIGINT AS raw_record_count,
                first(source_row_no ORDER BY {SORT_SQL})::BIGINT AS first_source_row_no,
                last(source_row_no ORDER BY {SORT_SQL})::BIGINT AS last_source_row_no
            FROM visit_rows
            GROUP BY did, chain_segment, visit_group
            """
        )
        connection.execute(
            """
            CREATE TABLE visits AS
            SELECT *,
                row_number() OVER (
                    PARTITION BY did, chain_segment ORDER BY visit_group
                )::INTEGER AS visit_order,
                row_number() OVER (
                    PARTITION BY did, chain_segment, city ORDER BY visit_group
                )::INTEGER AS city_visit_no
            FROM visits_base
            """
        )
        connection.execute(
            """
            CREATE TABLE chain_stats AS
            SELECT year, did, chain_segment, count(*)::BIGINT AS visit_count
            FROM visits
            GROUP BY year, did, chain_segment
            """
        )

        valid_records = int(_scalar(connection, "SELECT count(*) FROM valid_records"))
        chain_segments = int(_scalar(connection, "SELECT count(*) FROM chain_stats"))
        visit_count = int(_scalar(connection, "SELECT count(*) FROM visits"))
        max_visits = int(_scalar(connection, "SELECT max(visit_count) FROM chain_stats") or 0)
        expected_edges = int(
            _scalar(connection, "SELECT sum(visit_count * (visit_count - 1) / 2)::BIGINT FROM chain_stats") or 0
        )
        expected_level_one = int(
            _scalar(connection, "SELECT sum(greatest(visit_count - 1, 0))::BIGINT FROM chain_stats") or 0
        )
        expected_weight = float(
            _scalar(
                connection,
                "SELECT sum(2 * visit_count - 4 + pow(2, 2 - visit_count)) FROM chain_stats",
            )
            or 0.0
        )
        _assert_zero(
            connection,
            f"SELECT abs((SELECT sum(raw_record_count) FROM visits) - {valid_records})",
            f"{year} 年访问记录数回算失败",
        )
        _assert_zero(
            connection,
            """
            SELECT count(*) FROM visits a
            JOIN visits b
              ON a.did=b.did AND a.chain_segment=b.chain_segment
             AND b.visit_order=a.visit_order+1
            WHERE a.city=b.city
            """,
            f"{year} 年连续同城合并失败",
        )

        connection.execute(
            f"""
            CREATE TABLE edges AS
            SELECT
                concat(
                    s.year::VARCHAR, '-', s.did, '-', s.chain_segment::VARCHAR,
                    '-', s.visit_order::VARCHAR, '-', t.visit_order::VARCHAR
                ) AS edge_id,
                s.year,
                s.did,
                s.chain_segment,
                s.visit_order AS source_visit_order,
                t.visit_order AS target_visit_order,
                s.city AS source_city,
                t.city AS target_city,
                (t.visit_order - s.visit_order)::INTEGER AS edge_level,
                (t.visit_order - s.visit_order - 1)::INTEGER AS skipped_visit_count,
                pow({DECAY_BASE}, t.visit_order - s.visit_order - 1)::DOUBLE AS edge_weight,
                s.entry_time AS source_entry_time,
                s.exit_time AS source_exit_time,
                t.entry_time AS target_entry_time,
                t.exit_time AS target_exit_time,
                date_diff('second', s.exit_time, t.entry_time) / 3600.0 AS gap_hours,
                s.raw_record_count AS source_record_count,
                t.raw_record_count AS target_record_count,
                s.first_source_row_no AS source_first_row_no,
                s.last_source_row_no AS source_last_row_no,
                t.first_source_row_no AS target_first_row_no,
                t.last_source_row_no AS target_last_row_no,
                s.city_visit_no > 1 AS source_is_revisit,
                t.city_visit_no > 1 AS target_is_revisit,
                t.city_visit_no AS target_city_visit_no,
                (t.visit_order - s.visit_order = 1 AND t.city_visit_no > 1) AS is_direct_return_edge,
                s.city = t.city AS is_same_city_endpoint,
                date_diff('second', s.exit_time, t.entry_time) < 0 AS has_time_conflict,
                s.entry_time = t.entry_time AS has_same_start_time,
                '{input_rel}' AS source_file
            FROM visits s
            JOIN visits t
              ON s.did=t.did
             AND s.chain_segment=t.chain_segment
             AND s.visit_order<t.visit_order
            """
        )

        actual_edges = int(_scalar(connection, "SELECT count(*) FROM edges"))
        level_one_edges = int(_scalar(connection, "SELECT count(*) FROM edges WHERE edge_level=1"))
        actual_weight = float(_scalar(connection, "SELECT sum(edge_weight) FROM edges") or 0.0)
        if actual_edges != expected_edges:
            raise RuntimeError(f"{year} 年边数 {actual_edges:,} 与理论值 {expected_edges:,} 不一致。")
        if level_one_edges != expected_level_one:
            raise RuntimeError(
                f"{year} 年一级边数 {level_one_edges:,} 与理论值 {expected_level_one:,} 不一致。"
            )
        if abs(actual_weight - expected_weight) > max(1e-9, expected_weight * 1e-12):
            raise RuntimeError(f"{year} 年权重合计与链级公式不一致。")
        _assert_zero(
            connection,
            """
            SELECT count(*) FROM (
                SELECT year, did, chain_segment, source_visit_order, target_visit_order, count(*) n
                FROM edges GROUP BY ALL HAVING n>1
            )
            """,
            f"{year} 年候选主键重复",
        )
        _assert_zero(
            connection,
            f"""
            SELECT count(*) FROM edges
            WHERE edge_level < 1
               OR skipped_visit_count <> edge_level - 1
               OR abs(edge_weight - pow({DECAY_BASE}, edge_level - 1)) > 1e-15
               OR source_city IS NULL OR target_city IS NULL
               OR source_visit_order >= target_visit_order
            """,
            f"{year} 年边级、权重、方向或端点检查失败",
        )
        _assert_zero(
            connection,
            "SELECT count(*) FROM edges WHERE edge_level=1 AND is_same_city_endpoint",
            f"{year} 年一级边出现同城自环",
        )

        output_query = f"""
            SELECT
                edge_id AS "边记录编号",
                year AS "年份",
                did AS "农机编号",
                chain_segment AS "链段编号",
                source_visit_order AS "起点访问序号",
                target_visit_order AS "终点访问序号",
                source_city AS "起点城市",
                target_city AS "终点城市",
                edge_level AS "边级",
                skipped_visit_count AS "跨越城市数",
                edge_weight AS "衰减权重",
                source_entry_time AS "起点进入时间",
                source_exit_time AS "起点离开时间",
                target_entry_time AS "终点进入时间",
                target_exit_time AS "终点离开时间",
                gap_hours AS "时间间隔小时",
                source_record_count AS "起点原始记录数",
                target_record_count AS "终点原始记录数",
                source_first_row_no AS "起点首行号",
                source_last_row_no AS "起点末行号",
                target_first_row_no AS "终点首行号",
                target_last_row_no AS "终点末行号",
                source_is_revisit AS "起点是否回访",
                target_is_revisit AS "终点是否回访",
                target_city_visit_no AS "终点城市访问序次",
                is_direct_return_edge AS "是否直接回流边",
                is_same_city_endpoint AS "是否同城端点",
                has_time_conflict AS "是否时间冲突",
                has_same_start_time AS "是否同起点时间",
                source_file AS "来源文件"
            FROM edges
            ORDER BY year, did, chain_segment, source_visit_order, target_visit_order
        """
        connection.execute(
            f"COPY ({output_query}) TO '{_sql_path(csv_without_bom)}' "
            "(HEADER, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')"
        )
        _copy_with_bom(csv_without_bom, csv_with_bom)

        readback_count = int(
            _scalar(
                connection,
                f"SELECT count(*) FROM read_csv_auto('{_sql_path(csv_with_bom)}', header=true, all_varchar=true)",
            )
        )
        if readback_count != actual_edges:
            raise RuntimeError(f"{year} 年输出读回行数不一致。")
        if _read_header(csv_with_bom) != OUTPUT_COLUMNS:
            raise RuntimeError(f"{year} 年输出字段或字段顺序不一致。")

        same_city_edges = int(_scalar(connection, "SELECT count(*) FROM edges WHERE is_same_city_endpoint"))
        target_revisit_edges = int(_scalar(connection, "SELECT count(*) FROM edges WHERE target_is_revisit"))
        direct_return_edges = int(_scalar(connection, "SELECT count(*) FROM edges WHERE is_direct_return_edge"))
        time_conflict_edges = int(_scalar(connection, "SELECT count(*) FROM edges WHERE has_time_conflict"))
        same_start_edges = int(_scalar(connection, "SELECT count(*) FROM edges WHERE has_same_start_time"))
        input_hash = _sha256(input_path)
        output_hash = _sha256(csv_with_bom)

        metrics = [
            ("输入", "输入行数", input_rows, "行", "通过", "与数据字典登记行数一致"),
            ("输入", "唯一农机数", unique_did, "台", "通过", "按 did 去重"),
            ("输入", "轨迹城市数", city_count, "个", "通过", "保留全部非空城市，不限于81城"),
            ("输入", "日期最小值", date_min, "日期", "通过", ""),
            ("输入", "日期最大值", date_max, "日期", "通过", ""),
            ("缺失与异常", "did缺失", missing_did, "行", "通过", "为0；否则停止"),
            ("缺失与异常", "日期缺失或不可解析", invalid_date, "行", "通过", "为0；否则停止"),
            ("缺失与异常", "开始时间缺失或不可解析", invalid_start, "行", "通过", "为0；否则停止"),
            ("缺失与异常", "结束时间缺失或不可解析", invalid_end, "行", "通过", "为0；否则停止"),
            ("缺失与异常", "城市缺失", missing_city, "行", "已审计", "不填补；作为断点，不进入访问或边"),
            ("缺失与异常", "城市缺失块", missing_city_blocks, "块", "已审计", "每个连续缺失块打断接续链"),
            ("缺失与异常", "结束早于开始", reversed_time, "行", "通过", "为0；否则停止"),
            ("缺失与异常", "date与start_time日期不一致", date_time_mismatch, "行", "已审计", "不自动修改"),
            ("排序", "同机同开始时间异城组", same_start_diff_city_groups, "组", "已审计", "按确定性次序保留"),
            ("排序", "依赖原始行号打破的完全排序并列组", exact_sort_tie_groups, "组", "已审计", "不去重"),
            ("处理", "进入访问合并的有效记录", valid_records, "行", "通过", "输入行数减城市缺失行"),
            ("处理", "链段数", chain_segments, "段", "通过", "缺失城市块两侧不连接"),
            ("处理", "合并后访问数", visit_count, "次", "通过", "连续同城合并；离开后回访保留"),
            ("处理", "被连续同城折叠的记录数", valid_records - visit_count, "行", "通过", "未删除其计数信息"),
            ("处理", "单链最大访问数", max_visits, "次", "通过", "组合规模检查"),
            ("输出", "理论边数", expected_edges, "行", "通过", "逐链 L(L-1)/2 求和"),
            ("输出", "实际边数", actual_edges, "行", "通过", "与理论边数一致"),
            ("输出", "一级边数", level_one_edges, "行", "通过", "与逐链 max(L-1,0) 求和一致"),
            ("输出", "权重合计", format(actual_weight, ".15g"), "权重", "通过", "与逐链封闭公式一致"),
            ("回访", "目标为回访的边", target_revisit_edges, "行", "已审计", "仅表示链段内目标城市此前出现过"),
            ("回访", "直接回流边", direct_return_edges, "行", "已审计", "一级边且目标访问为回访"),
            ("回访", "同城端点边", same_city_edges, "行", "已审计", "离开后再次回到同城形成，予以保留"),
            ("时间", "时间冲突边", time_conflict_edges, "行", "已审计", "时间间隔小时<0，保留不筛"),
            ("时间", "同起点时间边", same_start_edges, "行", "已审计", "起点与终点访问进入时间相同，保留不筛"),
            ("追溯", "输入文件字节数", input_path.stat().st_size, "字节", "通过", ""),
            ("追溯", "输入文件SHA256", input_hash, "哈希", "通过", ""),
            ("追溯", "输出文件字节数", csv_with_bom.stat().st_size, "字节", "通过", "含UTF-8 BOM"),
            ("追溯", "输出文件SHA256", output_hash, "哈希", "通过", ""),
        ]
        for group, metric, value, unit, status, note in metrics:
            _audit_row(
                audit_rows,
                run_id,
                year,
                group,
                metric,
                value,
                unit,
                status,
                note,
                input_rel,
                output_rel,
            )
        return {
            "year": year,
            "input": input_rel,
            "output": output_rel,
            "input_rows": input_rows,
            "valid_records": valid_records,
            "missing_city": missing_city,
            "chain_segments": chain_segments,
            "visits": visit_count,
            "edges": actual_edges,
            "level_one_edges": level_one_edges,
            "same_city_endpoint_edges": same_city_edges,
            "direct_return_edges": direct_return_edges,
            "time_conflict_edges": time_conflict_edges,
            "output_bytes": csv_with_bom.stat().st_size,
            "input_sha256": input_hash,
            "output_sha256": output_hash,
            "temp_output": str(csv_with_bom),
        }
    finally:
        connection.close()


def _write_audit(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "运行编号",
        "运行时间",
        "年份",
        "指标组",
        "指标",
        "数值",
        "单位",
        "检查状态",
        "说明",
        "输入文件",
        "输出文件",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_all_years(project_root: Path, allow_overwrite: bool = False) -> dict[str, Any]:
    """生成三年输出；默认拒绝覆盖，允许覆盖时会先自动备份旧输出。"""
    project_root = project_root.resolve()
    inspect_inputs(project_root)
    run_unit_tests()
    output_paths = [(project_root / cfg["output"]).resolve() for cfg in YEAR_CONFIG.values()]
    audit_path = (project_root / AUDIT_RELATIVE_PATH).resolve()
    all_formal_paths = output_paths + [audit_path]
    existing = [path for path in all_formal_paths if path.exists()]
    if existing and not allow_overwrite:
        shown = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "以下正式输出已存在。为避免覆盖已停止；确认重跑时请将 "
            f"allow_overwrite 设为 True，代码会先备份：\n{shown}"
        )
    for path in all_formal_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
    backup_relative = _prepare_overwrite_backup(project_root, all_formal_paths) if existing else ""
    run_id = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%dT%H%M%S%z")
    audit_rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="城市接续_", dir=str(project_root / "Code" / "01_轨迹转城市接续")) as tmp:
        temp_dir = Path(tmp)
        summaries = [
            _process_year(project_root, temp_dir, year, config, run_id, audit_rows)
            for year, config in YEAR_CONFIG.items()
        ]
        script_path = Path(__file__).resolve()
        notebook_path = script_path.with_suffix(".ipynb")
        provenance_files = [
            (script_path, "生成脚本SHA256", "生成本次结果的模块文件"),
            (
                notebook_path,
                "主Notebook运行前SHA256",
                "Notebook保存执行输出后文件哈希可能变化；该值记录运行开始时的版本",
            ),
        ]
        for path, metric, note in provenance_files:
            if path.exists():
                _audit_row(
                    audit_rows,
                    run_id,
                    "全部",
                    "追溯",
                    metric,
                    _sha256(path),
                    "哈希",
                    "通过",
                    f"{note}：{str(path.relative_to(project_root)).replace('\\', '/')}",
                    "",
                    AUDIT_RELATIVE_PATH,
                )
        if backup_relative:
            _audit_row(
                audit_rows,
                run_id,
                "全部",
                "覆盖保护",
                "旧输出备份目录",
                backup_relative,
                "路径",
                "通过",
                "重跑前已备份所有既有正式输出",
                "",
                AUDIT_RELATIVE_PATH,
            )
        audit_temp = temp_dir / audit_path.name
        _write_audit(audit_temp, audit_rows)
        for summary, destination in zip(summaries, output_paths, strict=True):
            os.replace(summary["temp_output"], destination)
            summary.pop("temp_output")
        os.replace(audit_temp, audit_path)

    result = {
        "run_id": run_id,
        "write_date": WRITE_DATE,
        "decay_base": DECAY_BASE,
        "all_cities_retained": True,
        "maximum_gap_filter": None,
        "backup": backup_relative or None,
        "audit": AUDIT_RELATIVE_PATH,
        "years": summaries,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    build_all_years(Path(__file__).resolve().parents[2], allow_overwrite=False)
