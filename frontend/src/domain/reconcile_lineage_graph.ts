/** 对账血缘确认图纯前端领域逻辑：轮次版本推导、复核资格与追补清单汇总。
 *
 * 与后端 reconcile_lineage_graph 服务共用同一套业务规则：
 * 图节点把「历史批次 + 对账版本 + 暂存分区」钉成一次可回查的交付，
 * 补到文件只属于后轮，前轮复核不混读后轮分区。
 */

/** 确认图状态机，与后端 STATUS_ORDER 对齐。 */
export const GRAPH_STATUSES = ['草稿', '待确认', '复核中', '已确认', '已失效'] as const

/** 确认图可执行动作，与后端 ACTION_RULES 对齐。 */
export const GRAPH_ACTIONS = ['提交确认', '发起复核', '确认图谱', '标记失效'] as const

/** 由对账轮次推导对账版本：R1 → v1.0，R2 → v2.0。 */
export function deriveReconcileVersion(round: string | number): string {
  const text = String(round).replace(/^R/i, '')
  const no = Number.parseInt(text, 10)
  return Number.isFinite(no) && no > 0 ? `v${no}.0` : 'v1.0'
}

/** 判断某轮图节点是否可发起复核：已失效的图不允许复核，其余轮次保留原结果可回查。 */
export function canReview(status: string): boolean {
  return status !== '已失效'
}

/** 判断分区是否达到完整：记录数不少于应到条数。补单遇到另一份完整分区时改用它。 */
export function isCompletePartition(recordCount: number, expectedCount: number): boolean {
  return expectedCount > 0 && recordCount >= expectedCount
}

/** 汇总下一次要追补的清单：未覆盖分区去重后按名称排序，保证值班人员看到稳定顺序。 */
export function summarizeBacklog(uncovered: string[]): string[] {
  return Array.from(new Set(uncovered.filter((name) => name.trim().length > 0))).sort()
}
