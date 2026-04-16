<template>
  <!--<dv-full-screen-container>-->

  <div class="dashboard">
    <div class="row">
      <div class="card" style="width: 25%">
        <DistanceStatsChart />
      </div>
      <div class="card" style="width: 25%">
        <CategoryDistanceChart />
      </div>
      <div class="card" style="width: 25%">
        <CategoryDistanceBoxplotChart />
      </div>
      <div class="card" style="width: 25%">
        <DistanceFeeGroupChart />
      </div>
    </div>

    <div class="row">
      <div class="card" style="width: 20%">
        <DeliveryTimeDistributionChart />
      </div>
      <div class="card" style="width: 20%">
        <DeliveryFeeDistributionChart />
      </div>
      <div class="card" style="width: 20%">
        <FeeDistanceScatterChart />
      </div>
      <div class="card" style="width: 20%">
        <FeeTimeScatterChart />
      </div>
      <div class="card" style="width: 20%">
        <FeeTimeGroupChart />
      </div>
    </div>
  </div>
</template>

<script setup>
import DistanceStatsChart from '@/components/charts/DistanceStatsChart.vue'
import CategoryDistanceChart from '@/components/charts/CategoryDistanceChart.vue'
import CategoryDistanceBoxplotChart from '@/components/charts/CategoryDistanceBoxplotChart.vue'
import DeliveryTimeDistributionChart from '@/components/charts/DeliveryTimeDistributionChart.vue'
import DeliveryFeeDistributionChart from '@/components/charts/DeliveryFeeDistributionChart.vue'
import FeeDistanceScatterChart from '@/components/charts/FeeDistanceScatterChart.vue'
import FeeTimeScatterChart from '@/components/charts/FeeTimeScatterChart.vue'
import DistanceFeeGroupChart from '@/components/charts/DistanceFeeGroupChart.vue'
import FeeTimeGroupChart from '@/components/charts/FeeTimeGroupChart.vue'
</script>

<style scoped>
.dashboard {
  width: 100%;
  height: 80vh; /* 占满整个视口高度 */
  overflow: hidden; /* 隐藏溢出内容，禁止滚动 */
}
.row {
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
  flex-wrap: nowrap;
}
.card {
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  backdrop-filter: blur(5px);
  overflow: hidden;
}
</style>
