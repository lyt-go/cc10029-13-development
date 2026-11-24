"""对账血缘确认图领域服务：维护每轮对账与实际落地区段的版本化确认视图。

本模块把「历史批次 + 对账版本 + 暂存分区」在建图时刻钉成一次可回查的交付，
供值班人员从图上的对账版本发起复核。同一批次连续两轮对账结果不同时，
补到文件只属于后轮图节点；从前轮历史节点发出的复核请求（包括并发请求）
只读前轮钉住的落地区段，不会混读后轮分区。到达对账执行时优先读取本图
钉住的数据来源；从图发起补单时保存本次实际使用的数据与关联血缘。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ReconcileLineageGraphService(BaseModuleService):
    MODULE = "reconcile_lineage_graph"
    ENTITY = "确认图"
    OPERATION_TERM = "核对"
    LIST_FIELDS = ["图编号", "批次编号", "对账轮次", "对账版本", "暂存分区", "落地区段", "补单编号", "确认时间"]
    REQUIRED_FIELDS = ["图编号", "批次编号", "对账轮次"]
    STATUS_ORDER = ["草稿", "待确认", "复核中", "已确认", "已失效"]
    ACTION_RULES = {"提交确认": "待确认", "发起复核": "复核中", "确认图谱": "已确认", "标记失效": "已失效"}
    NEGATIVE_ACTIONS = ["标记失效"]

    # -- 图构建与查询 --

    def build_graph(self, batch_code: str, recon_code: str | None = None) -> dict[str, Any]:
        """为批次构建一轮对账血缘确认图：查询对账与血缘数据，钉住本轮实际落地区段。

        图节点把建图时刻的暂存分区文件清单固定下来，作为本轮对账版本的数据
        边界：后续轮次新增的补到文件不会混入本轮。若当前已存在达到应到条数
        的完整分区，本轮只钉住最新那份完整分区，避免与部分分区混读。
        """
        round_no = self._next_round_no(batch_code)
        partitions = self._pin_partitions(batch_code)
        lineage_refs = self._lineage_refs_for(batch_code)
        reconciles = self._reconcile_rows_for(batch_code)
        latest_recon = reconciles[-1] if reconciles else {}

        graph_code = f"GRAP-{batch_code[:8]}-R{round_no}-{time.time_ns()}"
        staging_codes = [Path(p["file"]).stem for p in partitions]
        segment_desc = ";".join(f"{p['file']}({p['record_count']}条)" for p in partitions) or "无落地区段"
        values = {
            "图编号": graph_code,
            "批次编号": batch_code,
            "对账轮次": f"R{round_no}",
            "对账版本": f"v{round_no}.0",
            "暂存分区": ",".join(staging_codes) or "无",
            "落地区段": segment_desc,
            "补单编号": "",
            "确认时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建确认图失败: {missing}")

        node = {
            "graph_code": graph_code,
            "batch_code": batch_code,
            "round_no": round_no,
            "reconcile_version": f"v{round_no}.0",
            "recon_code": recon_code or latest_recon.get("对账编号", ""),
            "reconcile_result": latest_recon.get("对账结论", ""),
            "partitions": partitions,
            "lineage_refs": lineage_refs,
            "pinned_at": time.time(),
        }
        graph_file = self._data_dir("graphs") / f"{graph_code}.jsonl"
        self._write_jsonl(graph_file, [node])
        self.run_action(entry["id"], "提交确认")

        return {
            "graph_code": graph_code,
            "graph_id": entry["id"],
            "batch_code": batch_code,
            "round_no": round_no,
            "reconcile_version": f"v{round_no}.0",
            "partitions": partitions,
            "lineage_refs": lineage_refs,
            "artifact_path": str(graph_file),
        }

    def query_graph(self, batch_code: str) -> dict[str, Any]:
        """查询批次的确认图：每轮对账版本、实际落地区段与关联血缘，供值班人员核对。"""
        rounds = []
        for entry in self._graphs_for(batch_code):
            node = self._load_node(entry) or {}
            rounds.append({
                "graph_id": entry["id"],
                "graph_code": entry["图编号"],
                "round_no": node.get("round_no"),
                "reconcile_version": entry["对账版本"],
                "status": entry["status"],
                "partitions": node.get("partitions", []),
                "lineage_refs": node.get("lineage_refs", []),
                "supplement_code": entry.get("补单编号", ""),
            })
        rounds.sort(key=lambda item: item.get("round_no") or 0)
        return {"batch_code": batch_code, "rounds": rounds, "count": len(rounds)}

    # -- 复核交付 --

    def start_review(self, graph_id: int) -> dict[str, Any]:
        """从图上的对账版本发起复核：只读本图钉住的落地区段，产出可回查的复核交付。

        补到文件只属于后轮图节点；从前轮历史节点发出的复核请求（包括并发
        请求）只读前轮钉住的文件，复核结果与建图时刻一致，不随后续轮次漂移。
        """
        entry = self.get_entry(graph_id)
        if entry is None:
            return {"error": f"确认图 {graph_id} 不存在"}
        if entry.get("status") == "已失效":
            return {"error": f"确认图 {entry['图编号']} 已失效，不能发起复核", "graph_id": graph_id}
        node = self._load_node(entry)
        if node is None:
            return {"error": f"确认图 {entry['图编号']} 缺少图产物", "graph_id": graph_id}

        self.run_action(graph_id, "发起复核")

        # 版本隔离的核心：只读取本图钉住的分区文件
        staging_dir = self._data_dir("staging")
        records: list[dict[str, Any]] = []
        files_read: list[str] = []
        for part in node.get("partitions", []):
            records.extend(self._read_jsonl(staging_dir / part["file"]))
            files_read.append(part["file"])

        review_code = f"REVI-{entry['图编号'][:8]}-{time.time_ns()}"
        delivery = {
            "review_code": review_code,
            "graph_code": entry["图编号"],
            "batch_code": entry["批次编号"],
            "round_no": node.get("round_no"),
            "reconcile_version": entry["对账版本"],
            "partitions_read": files_read,
            "records_read": len(records),
            "checksum": self._compute_checksum(str(records)),
            "lineage_refs": node.get("lineage_refs", []),
            "reviewed_at": time.time(),
        }
        # 复核交付持久化：历史批次+对账版本+暂存分区对应一次可回查的交付
        delivery_file = self._data_dir("reviews") / f"{review_code}.jsonl"
        self._write_jsonl(delivery_file, [delivery])
        self._write_checkpoint(f"review_{review_code}", delivery)
        return {**delivery, "artifact_path": str(delivery_file)}

    def get_review_delivery(self, review_code: str) -> dict[str, Any]:
        """回查复核交付：旧图仍可复核原结果，交付产物不随后续轮次变化。"""
        delivery_file = self._data_dir("reviews") / f"{review_code}.jsonl"
        records = self._read_jsonl(delivery_file)
        if not records:
            return {"error": f"复核交付 {review_code} 不存在", "review_code": review_code}
        return {"review_code": review_code, "delivery": records[0], "recheckable": True}

    # -- 下一轮对账与数据来源 --

    def create_next_round(self, graph_id: int, expected_count: int | None = None) -> dict[str, Any]:
        """用图新建下一轮对账：新轮次钉住本次实际处理的分区，旧轮次保持可复核。

        新轮次的图节点在建图时刻固定分区清单，到达对账执行时只读该清单，
        不会与旧轮次或其他批次的分区混读。
        """
        entry = self.get_entry(graph_id)
        if entry is None:
            return {"error": f"确认图 {graph_id} 不存在"}
        batch_code = entry["批次编号"]
        if expected_count is None:
            expected_count = self._expected_count_for(batch_code)

        ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
        recon = ar_svc.create_reconciliation(batch_code, expected_count)
        graph = self.build_graph(batch_code, recon_code=recon["recon_code"])
        return {
            "batch_code": batch_code,
            "new_recon": recon,
            "new_graph": graph,
            "round_no": graph["round_no"],
            "partitions": graph["partitions"],
        }

    def get_pinned_source(self, batch_code: str) -> list[str]:
        """返回批次最新有效图钉住的暂存分区文件名清单，供到达对账读取数据来源。

        已失效的图不参与数据来源选择；没有任何有效图时返回空清单，
        由调用方回退到默认的目录扫描行为。
        """
        graphs = [g for g in self._graphs_for(batch_code) if g.get("status") != "已失效"]
        if not graphs:
            return []

        def sort_key(entry: dict[str, Any]) -> tuple[int, int, int]:
            round_text = str(entry.get("对账轮次", "R0")).lstrip("R")
            round_no = int(round_text) if round_text.isdigit() else 0
            return (1 if entry.get("status") == "已确认" else 0, round_no, int(entry.get("id", 0)))

        latest = sorted(graphs, key=sort_key)[-1]
        node = self._load_node(latest)
        if not node:
            return []
        return [p["file"] for p in node.get("partitions", [])]

    # -- 补单与追补 --

    def create_supplement_order(self, graph_id: int) -> dict[str, Any]:
        """从图发起对账补单：保存本次实际使用的数据与关联血缘。

        补单以图钉住的分区为基础；若此刻暂存区出现图外的另一份完整分区
        （记录数达到应到条数），本次补单改用该完整分区的数据并标记来源。
        本次使用的数据快照与血缘引用写入补单产物，供事后核对与追补。
        """
        entry = self.get_entry(graph_id)
        if entry is None:
            return {"error": f"确认图 {graph_id} 不存在"}
        node = self._load_node(entry)
        if node is None:
            return {"error": f"确认图 {entry['图编号']} 缺少图产物", "graph_id": graph_id}
        batch_code = entry["批次编号"]

        pinned_files = [p["file"] for p in node.get("partitions", [])]
        expected = self._expected_count_for(batch_code)

        # 发现图外的另一份完整分区：补到文件达到应到条数时，本次补单改用它
        extra_complete = [
            part for part in self._partition_manifest(batch_code)
            if part["file"] not in pinned_files
            and expected > 0 and part["record_count"] >= expected
        ]
        use_files = [extra_complete[-1]["file"]] if extra_complete else pinned_files

        staging_dir = self._data_dir("staging")
        records: list[dict[str, Any]] = []
        for name in use_files:
            records.extend(self._read_jsonl(staging_dir / name))

        supplement_code = f"SUPP-{entry['图编号'][:8]}-{time.time_ns()}"
        lineage_refs = node.get("lineage_refs", []) or self._lineage_refs_for(batch_code)

        # 本次使用的数据快照单独落盘，清单文件记录元信息与关联血缘
        data_file = self._data_dir("supplements") / f"{supplement_code}_data.jsonl"
        self._write_jsonl(data_file, records)
        manifest = {
            "supplement_code": supplement_code,
            "graph_code": entry["图编号"],
            "batch_code": batch_code,
            "round_no": node.get("round_no"),
            "reconcile_version": entry["对账版本"],
            "source_partitions": use_files,
            "extra_complete_partitions": [p["file"] for p in extra_complete],
            "records_used": len(records),
            "data_checksum": self._compute_checksum(str(records)),
            "lineage_refs": lineage_refs,
            "data_file": data_file.name,
            "created_at": time.time(),
        }
        manifest_file = self._data_dir("supplements") / f"{supplement_code}.jsonl"
        self._write_jsonl(manifest_file, [manifest])
        entry["补单编号"] = supplement_code
        return {**manifest, "artifact_path": str(manifest_file), "data_path": str(data_file)}

    def activate_partition(self, graph_id: int) -> dict[str, Any]:
        """让该轮完整分区生效：确认图谱后，到达对账改读本轮钉住的数据来源。"""
        entry = self.get_entry(graph_id)
        if entry is None:
            return {"error": f"确认图 {graph_id} 不存在"}
        node = self._load_node(entry)
        if node is None:
            return {"error": f"确认图 {entry['图编号']} 缺少图产物", "graph_id": graph_id}
        final, msg = self.run_action(graph_id, "确认图谱")
        if final is None:
            return {"error": msg, "graph_id": graph_id}
        active_files = [p["file"] for p in node.get("partitions", [])]
        self._write_jsonl(
            self._data_dir("events") / "graph_activate_log.jsonl",
            [{"action": "activate_partition", "graph_code": entry["图编号"],
              "batch_code": entry["批次编号"], "round_no": node.get("round_no"),
              "partitions": active_files, "activated_at": time.time()}]
        )
        return {
            "graph_code": entry["图编号"],
            "batch_code": entry["批次编号"],
            "round_no": node.get("round_no"),
            "status": "已确认",
            "active_partitions": active_files,
        }

    def verify_supplement_reading(self, graph_id: int) -> dict[str, Any]:
        """核对补单实际读取的数据，并汇总下一次要追补的分区与批次。

        重读补单数据快照重新计算校验值，与补单清单比对，确认补单实际读取
        的内容没有漂移；同时盘点本批次尚未被已确认完整分区覆盖的分区、以及
        没有已确认完整分区的其他批次，作为下一次追补清单。
        """
        entry = self.get_entry(graph_id)
        if entry is None:
            return {"error": f"确认图 {graph_id} 不存在"}
        supplement_code = entry.get("补单编号", "")
        if not supplement_code:
            return {"error": f"确认图 {entry['图编号']} 尚未发起补单", "graph_id": graph_id}
        manifests = self._read_jsonl(self._data_dir("supplements") / f"{supplement_code}.jsonl")
        if not manifests:
            return {"error": f"补单 {supplement_code} 缺少清单产物", "graph_id": graph_id}
        manifest = manifests[-1]

        records = self._read_jsonl(self._data_dir("supplements") / manifest["data_file"])
        actual_checksum = self._compute_checksum(str(records))
        reading = {
            "supplement_code": supplement_code,
            "records_read": len(records),
            "expected_records": manifest["records_used"],
            "checksum_match": actual_checksum == manifest["data_checksum"],
            "actual_checksum": actual_checksum,
            "manifest_checksum": manifest["data_checksum"],
            "source_partitions": manifest["source_partitions"],
        }

        batch_code = entry["批次编号"]
        uncovered = [] if self._has_confirmed_complete(batch_code) else [
            p["file"] for p in self._partition_manifest(batch_code)
            if p["file"] not in self._confirmed_files(batch_code)
        ]
        backlog_batches = sorted({
            str(row.get("批次编号", "")) for row in store.rows("transfer_batch")
            if row.get("批次编号") and not self._has_confirmed_complete(str(row["批次编号"]))
        })
        return {
            "graph_code": entry["图编号"],
            "batch_code": batch_code,
            "reading": reading,
            "uncovered_partitions": sorted(uncovered),
            "backlog_batches": backlog_batches,
        }

    # -- 内部帮助器 --

    def _graphs_for(self, batch_code: str) -> list[dict[str, Any]]:
        """列出批次在 store 中的全部确认图条目。"""
        return [row for row in store.rows(self.MODULE) if row.get("批次编号") == batch_code]

    def _reconcile_rows_for(self, batch_code: str) -> list[dict[str, Any]]:
        """列出批次的全部对账单条目。"""
        return [row for row in store.rows("arrival_reconcile") if row.get("批次编号") == batch_code]

    def _next_round_no(self, batch_code: str) -> int:
        """按批次已有图节点推导下一轮对账轮次号。"""
        round_nos = []
        for entry in self._graphs_for(batch_code):
            text = str(entry.get("对账轮次", "")).lstrip("R")
            if text.isdigit():
                round_nos.append(int(text))
        return max(round_nos, default=0) + 1

    def _load_node(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """读取图条目对应的图产物节点，产物缺失时返回 None。"""
        graph_file = self._data_dir("graphs") / f"{entry['图编号']}.jsonl"
        nodes = self._read_jsonl(graph_file)
        return nodes[0] if nodes else None

    def _staging_files_for(self, batch_code: str) -> list[Path]:
        """列出批次当前暂存区中的全部落地产物文件。"""
        return sorted(self._data_dir("staging").glob(f"*{batch_code}*.jsonl"))

    def _partition_manifest(self, batch_code: str) -> list[dict[str, Any]]:
        """汇总批次当前暂存分区的文件清单：文件名、记录数、校验值。"""
        manifest = []
        for path in self._staging_files_for(batch_code):
            records = self._read_jsonl(path)
            manifest.append({
                "file": path.name,
                "record_count": len(records),
                "checksum": self._compute_checksum(str(records)),
            })
        return manifest

    def _pin_partitions(self, batch_code: str) -> list[dict[str, Any]]:
        """钉住本轮实际处理的分区：存在完整分区时只取最新一份，避免混读。"""
        manifest = self._partition_manifest(batch_code)
        expected = self._expected_count_for(batch_code)
        complete = [p for p in manifest if expected > 0 and p["record_count"] >= expected]
        if complete:
            return [complete[-1]]
        return manifest

    def _expected_count_for(self, batch_code: str) -> int:
        """推导批次应到条数：取对账单应到条数上限，缺失时回退到批次记录条数。"""
        expected = 0
        for row in self._reconcile_rows_for(batch_code):
            try:
                expected = max(expected, int(row.get("应到条数", 0) or 0))
            except (TypeError, ValueError):
                continue
        if expected == 0:
            for row in store.rows("transfer_batch"):
                if row.get("批次编号") == batch_code:
                    try:
                        expected = int(row.get("记录条数", 0) or 0)
                    except (TypeError, ValueError):
                        expected = 0
                    break
        return expected

    def _lineage_refs_for(self, batch_code: str) -> list[dict[str, Any]]:
        """查询批次的血缘链路：store 中的血缘条目与血缘产物文件一并汇总。"""
        refs = []
        for row in store.rows("transfer_lineage"):
            if row.get("传输批次") == batch_code:
                refs.append({
                    "链路编号": row.get("链路编号", ""),
                    "血缘版本": row.get("血缘版本", ""),
                    "状态": row.get("status", ""),
                })
        lineage_dir = self._data_dir("lineage")
        for path in sorted(lineage_dir.glob(f"*{batch_code[:8]}*.jsonl")):
            for rec in self._read_jsonl(path):
                if rec.get("transfer_batch") == batch_code:
                    refs.append({
                        "链路编号": rec.get("lineage_code", ""),
                        "血缘版本": "artifact",
                        "状态": "已生效" if rec.get("balanced") else "存在差异",
                    })
        return refs

    def _confirmed_files(self, batch_code: str) -> set[str]:
        """汇总批次所有已确认图钉住的分区文件名。"""
        files: set[str] = set()
        for entry in self._graphs_for(batch_code):
            if entry.get("status") != "已确认":
                continue
            node = self._load_node(entry) or {}
            files.update(p["file"] for p in node.get("partitions", []))
        return files

    def _has_confirmed_complete(self, batch_code: str) -> bool:
        """判断批次是否已有已确认图钉住达到应到条数的完整分区。"""
        expected = self._expected_count_for(batch_code)
        if expected <= 0:
            return False
        for entry in self._graphs_for(batch_code):
            if entry.get("status") != "已确认":
                continue
            node = self._load_node(entry) or {}
            if any(p.get("record_count", 0) >= expected for p in node.get("partitions", [])):
                return True
        return False
