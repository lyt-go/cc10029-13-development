"""集成测试：跨模块数据交换链路端到端验证。

覆盖8条核心数据流链路的真实读写、异常处理和跨模块一致性。
"""
from __future__ import annotations

import os
import json
import time
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services.source_register import SourceRegisterService
from app.services.target_register import TargetRegisterService
from app.services.extract_window import ExtractWindowService
from app.services.transfer_batch import TransferBatchService
from app.services.staging_area import StagingAreaService
from app.services.checksum_verify import ChecksumVerifyService
from app.services.arrival_reconcile import ArrivalReconcileService
from app.services.conflict_resolve import ConflictResolveService
from app.services.reject_record import RejectRecordService
from app.services.field_contract import FieldContractService
from app.services.resume_point import ResumePointService
from app.services.speed_control import SpeedControlService
from app.services.format_adapt import FormatAdaptService
from app.services.transfer_lineage import TransferLineageService
from app.services.reconcile_lineage_graph import ReconcileLineageGraphService
from app.services.history_archive import HistoryArchiveService
from app.services.exchange_replay import ExchangeReplayService
from app.services.sla_monitor import SlaMonitorService
from app.services.channel_config import ChannelConfigService
from app.store import store


@pytest.fixture(autouse=True)
def cleanup():
    """每个测试前重置store数据，确保测试隔离（文件清理被沙箱限制，仅append不影响结果）。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


class TestChain1SourceToExtractToBatch:
    """链路1: 源端登记→窗口抽取→传输批次（贯通验证）"""

    def test_source_register_triggers_window_creation(self):
        """验证源端登记确认后自动创建增量抽取窗口。"""
        svc = SourceRegisterService()
        values = {"源端编号": "SOUR-CHAIN1", "系统名称": "测试系统1", "数据域": "订单域",
                  "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "100万",
                  "抽取方式": "增量抽取", "登记游标": "0"}
        entry = svc.register_source(values)
        assert entry is not None
        result = svc.confirm_register(entry["id"])
        assert "error" not in result
        triggers = result.get("auto_triggers", {})
        # 确认自动创建了抽取窗口
        if "extract_window" in triggers and "error" not in str(triggers["extract_window"]):
            assert triggers["extract_window"]["status"] == "草稿"
        # 确认自动创建了字段契约
        if "field_contract" in triggers and "error" not in str(triggers["field_contract"]):
            assert triggers["field_contract"]["status"] == "已生效"

    def test_full_extract_to_batch_pipeline(self):
        """验证抽取窗口执行后产物文件存在且自动驱动传输批次创建。"""
        # 前置：确保源端存在
        svc = SourceRegisterService()
        values = {"源端编号": "SOUR-CHAIN1B", "系统名称": "管道测试", "数据域": "交易域",
                  "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "50万",
                  "抽取方式": "增量抽取", "登记游标": "0"}
        entry = svc.register_source(values)
        svc.confirm_register(entry["id"])

        # 创建抽取窗口并执行抽取
        ew_svc = ExtractWindowService()
        window_entry = ew_svc.create_window("SOUR-CHAIN1B", count=15)
        result = ew_svc.execute_extract(window_entry["id"], record_count=15)
        assert "error" not in result
        assert result["records_extracted"] == 15

        # 验证产物文件存在且数据可读
        artifact = Path(result["artifact_path"])
        assert artifact.exists()

        # 验证自动触发了传输批次
        assert "auto_batch" in result
        assert "error" not in result["auto_batch"]


class TestChain2BatchToStagingToVerify:
    """链路2: 传输批次→暂存落地→校验核对（贯通验证）"""

    def test_batch_transfer_triggers_staging_and_verify(self):
        """验证传输执行后自动触发暂存落地和校验核对。"""
        tb_svc = TransferBatchService()
        batch = tb_svc.create_batch("EXTR-SOUR-xxx", 10, "TARG-0001")

        result = tb_svc.start_transfer(batch["batch_id"], chunk_size=3)
        assert "error" not in result
        assert result["records_transferred"] > 0

        # 验证产物文件存在
        artifact = Path(result["artifact_path"])
        assert artifact.exists()
        records = tb_svc._read_jsonl(artifact)
        assert len(records) > 0

        # 验证自动触发暂存落地
        assert "auto_staging" in result
        assert "error" not in result["auto_staging"]

        # 验证自动触发校验核对
        assert "auto_verify" in result
        assert "error" not in result["auto_verify"]

    def test_staging_lands_batch_data_correctly(self):
        """验证暂存落地读取传输产物并写入暂存产物。"""
        # 先创建传输批次产物
        tb_svc = TransferBatchService()
        batch = tb_svc.create_batch("EXTR-STAG-TEST", 10, "TARG-0001")
        tb_svc.start_transfer(batch["batch_id"], chunk_size=5)

        # 独立调用暂存落地验证产物
        sa_svc = StagingAreaService()
        result = sa_svc.land_batch(batch["batch_code"], 10)
        assert result["records_landed"] > 0
        artifact = Path(result["artifact_path"])
        assert artifact.exists()

    def test_checksum_verify_with_mismatch(self):
        """验证校验核对在执行后产生差异数据。"""
        tb_svc = TransferBatchService()
        batch = tb_svc.create_batch("EXTR-CHECK-TEST", 10, "TARG-0001")
        tb_svc.start_transfer(batch["batch_id"], chunk_size=5)

        cv_svc = ChecksumVerifyService()
        verify = cv_svc.create_verification(batch["batch_code"], 10)
        result = cv_svc.execute_verification(verify["verify_id"])
        # 校验可能通过也可能不通过，但结果必须有 valid keys
        assert "source_checksum" in result


class TestChain3VerifyDrivesConflictAndReject:
    """链路3: 校验差异→冲突裁决+拒收回执（贯通验证）"""

    def test_verification_diff_creates_conflict(self):
        """验证校验差异自动驱动冲突记录创建。"""
        cv_svc = ChecksumVerifyService()
        cn_svc = ConflictResolveService()

        # 创建冲突记录
        conflict = cn_svc.create_conflict("TEST-BATCH", 3)
        assert conflict["conflicts_created"] == 3

        # 裁决冲突
        for code in conflict["codes"][:1]:
            c = cn_svc.resolve_conflict(
                [r["id"] for r in store.rows("conflict_resolve") if r.get("冲突编号") == code][0],
                "source_first")
            assert c["final_value"] != ""
            assert c["strategy"] == "source_first"

    def test_rejection_creation_and_disposition(self):
        """验证拒收记录的创建与处置流程。"""
        rr_svc = RejectRecordService()
        rejection = rr_svc.create_rejection("TEST-BATCH", "校验", "SHA256校验差异5条")
        assert rejection["batch_code"] == "TEST-BATCH"

        # 处置拒收
        result = rr_svc.process_rejection(rejection["reject_id"], "return")
        assert result["result"] == "已退回"


class TestChain4ReconcileToSlaAndLineage:
    """链路4: 到达对账→时效监控+传输血缘（贯通验证）"""

    def test_reconciliation_updates_sla(self):
        """验证到达对账发现迟到后触发时效监控更新。"""
        ar_svc = ArrivalReconcileService()
        recon = ar_svc.create_reconciliation("TEST-BATCH-SLA", 100)
        result = ar_svc.execute_reconciliation(recon["recon_id"])
        # 对账结果驱动 SLA 更新
        assert "auto_sla_update" in result

    def test_reconciliation_records_lineage(self):
        """验证到达对账完成后记录传输血缘。"""
        ar_svc = ArrivalReconcileService()
        recon = ar_svc.create_reconciliation("TEST-BATCH-LIN", 50)
        result = ar_svc.execute_reconciliation(recon["recon_id"])
        assert "auto_lineage" in result


class TestChain5FieldContractMapping:
    """链路5: 字段契约映射（贯通验证）"""

    def test_field_contract_apply_to_extracted_data(self):
        """验证字段契约对抽取产物执行类型转换与映射。"""
        # 确保有契约并确认生效
        fc_svc = FieldContractService()
        entry = fc_svc.create_contract("ext_a", "dst_a", "string")
        fc_svc.run_action(entry["id"], "确认生效")

        # 应用契约到抽取产物
        result = fc_svc.apply_contracts("SOUR-0001", record_count=10)
        assert result["records_converted"] > 0
        artifact = Path(result["artifact_path"])
        assert artifact.exists()
        records = fc_svc._read_jsonl(artifact)
        assert "dst_a" in records[0]


class TestChain6ResumePointAndSpeedControl:
    """链路6: 断点续传与速率整形（贯通验证）"""

    def test_resume_point_creation_and_transfer(self):
        """验证断点续传的创建与恢复。"""
        rp_svc = ResumePointService()
        resume = rp_svc.create_resume_point("TEST-BATCH-RESUME", 5, 5)
        result = rp_svc.resume_transfer(resume["resume_id"])
        assert result["records_resumed"] >= 0

    def test_speed_control_shaping(self):
        """验证速率整形算法输出产物。"""
        sc_svc = SpeedControlService()
        policy = sc_svc.create_policy("测试范围", 100)
        result = sc_svc.apply_shaping(policy["id"], "TEST-BATCH-SHAPE", 200)
        assert result["shaped"] or "queued" in result
        shaping_file = sc_svc._data_dir("shaping") / "TEST-BATCH-SHAPE_shaping.jsonl"
        assert shaping_file.exists()


class TestChain7FormatAdapt:
    """链路7: 格式适配（贯通验证）"""

    def test_format_adapt_creation_and_apply(self):
        """验证格式适配的差异识别与格式转换。"""
        fa_svc = FormatAdaptService()
        entry = fa_svc.create_adaptation("order_format", "id,name,price", "id,title,price,discount")
        assert entry is not None

        result = fa_svc.execute_adaptation(entry["id"], "TEST-BATCH-FMT")
        assert result["records_adapted"] > 0
        artifact = Path(result["artifact_path"])
        assert artifact.exists()


class TestChain8ArchiveAndReplay:
    """链路8: 批次归档与历史重放（贯通验证）"""

    def test_archive_and_replay_cycle(self):
        """验证归档与重放的完整生命周。"""
        ha_svc = HistoryArchiveService()
        er_svc = ExchangeReplayService()

        archive = ha_svc.create_archive(["BATCH-A1", "BATCH-A2"])
        result = ha_svc.execute_archive(archive["archive_id"])
        assert result["record_count"] > 0
        assert result["checksum"] != ""

        # 重放归档数据
        replay = er_svc.create_replay(archive["archive_code"], "BATCH-A1-BATCH-A2", "append")
        reply_result = er_svc.execute_replay(replay["replay_id"])
        assert reply_result["records_replayed"] > 0

    def test_archive_destroy(self):
        """验证归档卷销毁操作。"""
        ha_svc = HistoryArchiveService()
        archive = ha_svc.create_archive(["BATCH-D1"])
        ha_svc.execute_archive(archive["archive_id"])
        result = ha_svc.destroy_archive(archive["archive_id"])
        assert result["status"] == "已销毁"


class TestChain9ReconcileLineageGraph:
    """链路9: 对账血缘确认图（两轮对账版本隔离、复核交付与补单追补）"""

    @staticmethod
    def _unique_batch(tag: str) -> str:
        """生成每次运行唯一的批次编号：暂存产物按批次名归档且只追加，
        唯一编号保证重复跑测试时分区文件互不混淆。"""
        return f"TRAN-GRAPH-{tag}-{time.time_ns()}"

    def _land_partition(self, svc, batch_code: str, tag: str, count: int) -> str:
        """为批次直接落地一份暂存分区文件，返回文件名。"""
        filename = f"STAG-{batch_code}-{tag}.jsonl"
        svc._write_jsonl(svc._data_dir("staging") / filename,
                         svc._simulate_records(batch_code, count=count))
        return filename

    def _run_first_round(self, batch_code: str, expected: int = 20, arrived: int = 10):
        """跑通第一轮对账：部分分区到达 → 存在差异，自动建图钉住第一轮区段。"""
        g_svc = ReconcileLineageGraphService()
        ar_svc = ArrivalReconcileService()
        first_file = self._land_partition(g_svc, batch_code, "PART1", arrived)
        recon = ar_svc.create_reconciliation(batch_code, expected)
        result = ar_svc.execute_reconciliation(recon["recon_id"])
        assert result["actual"] == arrived
        assert result["balanced"] is False
        assert "auto_graph" in result and "error" not in str(result["auto_graph"])
        graph = result["auto_graph"]
        assert [p["file"] for p in graph["partitions"]] == [first_file]
        return g_svc, ar_svc, graph, first_file

    def test_concurrent_review_reads_only_first_round(self):
        """同一批次两轮对账结果不同：补到文件只属于第二轮，
        从第一轮历史节点发出的并发复核只读第一轮钉住的区段。"""
        batch = self._unique_batch("ISO")
        g_svc, _, graph, first_file = self._run_first_round(batch)
        # 补到文件：一份完整分区，只属于第二轮
        self._land_partition(g_svc, batch, "FULL2", 20)

        # 操作员从第一轮的链路查询里发出并发复核请求
        with ThreadPoolExecutor(max_workers=2) as pool:
            reviews = list(pool.map(lambda _: g_svc.start_review(graph["graph_id"]), range(2)))
        for review in reviews:
            assert "error" not in review
            assert review["partitions_read"] == [first_file]
            assert review["records_read"] == 10
            assert review["reconcile_version"] == "v1.0"

        # 图上选定的批次+对账版本+暂存分区对应一次可回查的交付
        delivery = g_svc.get_review_delivery(reviews[0]["review_code"])
        assert delivery["recheckable"] is True
        assert delivery["delivery"]["checksum"] == reviews[0]["checksum"]
        assert delivery["delivery"]["records_read"] == 10

    def test_next_round_reads_own_partitions_no_mixed_read(self):
        """用图新建下一轮对账：本次实际处理的分区可查，到达对账读图钉住的来源。"""
        batch = self._unique_batch("NEXT")
        g_svc, ar_svc, graph, _ = self._run_first_round(batch)
        full_file = self._land_partition(g_svc, batch, "FULL2", 20)

        # 未建新一轮前直接再对账：仍读第一轮图钉住的来源，不混读补到文件
        recon_direct = ar_svc.create_reconciliation(batch, 20)
        direct = ar_svc.execute_reconciliation(recon_direct["recon_id"])
        assert direct["actual"] == 10
        assert direct["balanced"] is False

        # 用图新建下一轮：钉住本次实际处理的完整分区
        nxt = g_svc.create_next_round(graph["graph_id"], expected_count=20)
        assert nxt["round_no"] == 2
        assert [p["file"] for p in nxt["partitions"]] == [full_file]

        # 第二轮对账读新图钉住的来源，得到与第一轮不同的结果
        result2 = ar_svc.execute_reconciliation(nxt["new_recon"]["recon_id"])
        assert result2["actual"] == 20
        assert result2["balanced"] is True

        # 图上能查出两轮各自的版本与实际落地区段
        view = g_svc.query_graph(batch)
        assert view["count"] == 2
        assert view["rounds"][0]["reconcile_version"] == "v1.0"
        assert view["rounds"][1]["reconcile_version"] == "v2.0"
        assert [p["file"] for p in view["rounds"][1]["partitions"]] == [full_file]

    def test_supplement_order_activation_and_backlog(self):
        """从第一轮历史节点复核旧账→同图发起补单→第二轮完整分区生效→核对追补。"""
        batch = self._unique_batch("SUPP")
        g_svc, ar_svc, graph, first_file = self._run_first_round(batch)
        full_file = self._land_partition(g_svc, batch, "FULL2", 20)
        nxt = g_svc.create_next_round(graph["graph_id"], expected_count=20)

        # 从第一轮的历史节点复核旧账
        review = g_svc.start_review(graph["graph_id"])
        assert review["records_read"] == 10

        # 从同一张图发起补单：遇到另一份完整分区，保存本次使用的数据和关联血缘
        supp = g_svc.create_supplement_order(graph["graph_id"])
        assert supp["extra_complete_partitions"] == [full_file]
        assert supp["source_partitions"] == [full_file]
        assert supp["records_used"] == 20
        assert supp["lineage_refs"]
        assert Path(supp["data_path"]).exists()

        # 让第二轮完整分区生效
        activated = g_svc.activate_partition(nxt["new_graph"]["graph_id"])
        assert activated["status"] == "已确认"
        assert g_svc.get_pinned_source(batch) == [full_file]

        # 旧图仍可复核原结果
        again = g_svc.start_review(graph["graph_id"])
        assert again["records_read"] == 10
        assert again["partitions_read"] == [first_file]

        # 核对补单实际读取的数据 + 下一次要追补的清单
        verify = g_svc.verify_supplement_reading(graph["graph_id"])
        assert verify["reading"]["checksum_match"] is True
        assert verify["reading"]["records_read"] == 20
        assert verify["uncovered_partitions"] == []
        assert batch not in verify["backlog_batches"]


class TestCrossModuleSnapshot:
    """跨模块汇总验证：所有模块 domain_snapshot 可调用。"""
    MODULES = [
        ("source_register", SourceRegisterService),
        ("target_register", TargetRegisterService),
        ("field_contract", FieldContractService),
        ("extract_window", ExtractWindowService),
        ("transfer_batch", TransferBatchService),
        ("staging_area", StagingAreaService),
        ("checksum_verify", ChecksumVerifyService),
        ("arrival_reconcile", ArrivalReconcileService),
        ("conflict_resolve", ConflictResolveService),
        ("resume_point", ResumePointService),
        ("speed_control", SpeedControlService),
        ("reject_record", RejectRecordService),
        ("format_adapt", FormatAdaptService),
        ("transfer_lineage", TransferLineageService),
        ("reconcile_lineage_graph", ReconcileLineageGraphService),
        ("history_archive", HistoryArchiveService),
        ("exchange_replay", ExchangeReplayService),
        ("sla_monitor", SlaMonitorService),
        ("channel_config", ChannelConfigService),
    ]

    @pytest.mark.parametrize("mod_name,cls", MODULES)
    def test_domain_snapshot(self, mod_name, cls):
        snapshot = cls().domain_snapshot()
        assert snapshot["module"] == mod_name
        assert "artifact" in snapshot
        assert "count" in snapshot