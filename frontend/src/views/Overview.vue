<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "源端登记", "created": 0, "pending": 0, "abnormal": 0}, {"name": "目标端登记", "created": 0, "pending": 0, "abnormal": 0}, {"name": "字段契约", "created": 0, "pending": 0, "abnormal": 0}, {"name": "增量窗口", "created": 0, "pending": 0, "abnormal": 0}, {"name": "传输批次", "created": 0, "pending": 0, "abnormal": 0}, {"name": "暂存落地", "created": 0, "pending": 0, "abnormal": 0}, {"name": "校验核对", "created": 0, "pending": 0, "abnormal": 0}, {"name": "到达对账", "created": 0, "pending": 0, "abnormal": 0}, {"name": "字段冲突", "created": 0, "pending": 0, "abnormal": 0}, {"name": "断点续传", "created": 0, "pending": 0, "abnormal": 0}, {"name": "速率整形", "created": 0, "pending": 0, "abnormal": 0}, {"name": "拒收回执", "created": 0, "pending": 0, "abnormal": 0}, {"name": "格式适配", "created": 0, "pending": 0, "abnormal": 0}, {"name": "传输血缘", "created": 0, "pending": 0, "abnormal": 0}, {"name": "对账血缘确认图", "created": 0, "pending": 0, "abnormal": 0}, {"name": "批次归档", "created": 0, "pending": 0, "abnormal": 0}, {"name": "历史重放", "created": 0, "pending": 0, "abnormal": 0}, {"name": "时效监控", "created": 0, "pending": 0, "abnormal": 0}, {"name": "通道配置", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
