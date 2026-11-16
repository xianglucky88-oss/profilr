from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .baseline import BaselineError, BaselineStore
from .pipeline import (
    compare_versions,
    get_comparison_result,
    list_versions,
    reevaluate,
    rollback_baseline,
    save_baseline,
)
from .profile import input_identity, profile_rows, read_csv

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = "/tmp/profilr/profilr.db"


def _print(payload) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _load_spec(path: str | None) -> dict | None:
    if not path:
        return None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def demo() -> dict:
    """Two batches → two versions → one reproducible comparison."""
    store = BaselineStore(DEFAULT_DB)
    try:
        saved_a = save_baseline(
            store, "orders", profile_rows(read_csv(ROOT / "data" / "batch_a.csv")),
            input_identity(ROOT / "data" / "batch_a.csv"),
        )
        saved_b = save_baseline(
            store, "orders", profile_rows(read_csv(ROOT / "data" / "batch_b.csv")),
            input_identity(ROOT / "data" / "batch_b.csv"),
        )
        comparison = compare_versions(store, "orders", saved_a["version"], saved_b["version"])
        return {
            "table": "orders",
            "db_path": DEFAULT_DB,
            "baseline_version": saved_a["version"],
            "current_version": saved_b["version"],
            "baseline_created": saved_a["created"],
            "current_created": saved_b["created"],
            "baseline": store.load("orders", saved_a["version"]),
            "current": store.load("orders", saved_b["version"]),
            "comparison_id": comparison["comparison_id"],
            "compared_versions": comparison["compared_versions"],
            "spec": comparison["spec"],
            "comparison_reused": comparison["reused"],
            "schema_events": comparison["schema_events"],
            "metrics": comparison["field_metrics"],
            "alerts": comparison["alerts"],
            "summary": comparison["summary"],
        }
    finally:
        store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="profilr", description="版本化基线与漂移比较")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    p_profile = sub.add_parser("profile", help="剖析 CSV 并保存为新基线版本（同输入幂等）")
    p_profile.add_argument("table")
    p_profile.add_argument("csv")
    p_profile.add_argument("--input-id", help="输入身份；缺省为文件内容哈希")

    p_compare = sub.add_parser("compare", help="比较两个版本（缺省：head 与其前一版本）")
    p_compare.add_argument("table")
    p_compare.add_argument("--from", dest="from_version", type=int)
    p_compare.add_argument("--to", dest="to_version", type=int)
    p_compare.add_argument("--spec", help="比较口径 JSON 文件（阈值/分箱）")

    p_versions = sub.add_parser("versions", help="列出表的版本与当前 head")
    p_versions.add_argument("table")

    p_rollback = sub.add_parser("rollback", help="将 head 回退到指定历史版本")
    p_rollback.add_argument("table")
    p_rollback.add_argument("--to", dest="to_version", type=int, required=True)

    p_reeval = sub.add_parser("reevaluate", help="用新阈值重新解释已完成的比较")
    p_reeval.add_argument("table")
    p_reeval.add_argument("comparison_id")
    p_reeval.add_argument("--spec", help="新的比较口径 JSON 文件")

    p_result = sub.add_parser("result", help="查看已存储的比较结果")
    p_result.add_argument("comparison_id")

    p_serve = sub.add_parser("serve", help="启动只读 HTTP 查询服务")
    p_serve.add_argument("--port", type=int, default=9800)

    sub.add_parser("demo", help="内置两批数据的端到端演示")

    args = parser.parse_args(argv)

    if args.command == "demo":
        _print(demo())
        return 0
    if args.command == "serve":
        from .api import serve
        serve(args.db, port=args.port)
        return 0

    store = BaselineStore(args.db)
    try:
        if args.command == "profile":
            identity = args.input_id or input_identity(args.csv)
            _print(save_baseline(store, args.table, profile_rows(read_csv(args.csv)), identity))
        elif args.command == "compare":
            _print(compare_versions(store, args.table, args.from_version, args.to_version, _load_spec(args.spec)))
        elif args.command == "versions":
            _print(list_versions(store, args.table))
        elif args.command == "rollback":
            _print(rollback_baseline(store, args.table, args.to_version))
        elif args.command == "reevaluate":
            _print(reevaluate(store, args.table, args.comparison_id, _load_spec(args.spec)))
        elif args.command == "result":
            _print(get_comparison_result(store, args.comparison_id))
        return 0
    except BaselineError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
