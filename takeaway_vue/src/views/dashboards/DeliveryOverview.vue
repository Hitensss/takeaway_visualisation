<template>
  <div class="dashboard">
    <div class="row">
      <div class="card" style="width: 25%">
        <DeliveryFeeRatingGroupChart />
      </div>
      <div class="card" style="width: 25%">
        <DeliveryFeeSalesGroupChart />
      </div>
      <div class="card" style="width: 25%">
        <DistanceSalesGroupChart />
      </div>
      <div class="card" style="width: 25%">
        <MinPriceSummaryCard />
      </div>
    </div>

    <div class="row">
      <div class="card" style="width: 25%">
        <MinPriceRatingGroupChart />
      </div>
      <div class="card" style="width: 25%">
        <DeliveryTimeRatingGroupChart />
      </div>

      <div class="card" style="width: 25%">
        <DeliveryTimeSalesGroupChart />
      </div>

      <div class="card" style="width: 25%">
        <MinPriceDistributionChart />
      </div>
    </div>
  </div>
</template>

<script setup>
import DeliveryFeeRatingGroupChart from '@/components/charts/DeliveryFeeRatingGroupChart.vue'
import DeliveryTimeRatingGroupChart from '@/components/charts/DeliveryTimeRatingGroupChart.vue'
import MinPriceSummaryCard from '@/components/charts/MinPriceSummaryCard.vue'
import MinPriceRatingGroupChart from '@/components/charts/MinPriceRatingGroupChart.vue'
import DeliveryFeeSalesGroupChart from '@/components/charts/DeliveryFeeSalesGroupChart.vue'
import DeliveryTimeSalesGroupChart from '@/components/charts/DeliveryTimeSalesGroupChart.vue'
import DistanceSalesGroupChart from '@/components/charts/DistanceSalesGroupChart.vue'
import MinPriceDistributionChart from '@/components/charts/MinPriceDistributionChart.vue'
</script>

<style scoped>
.dashboard {
  width: 100%;
}
.row {
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
  flex-wrap: nowrap;
}
.card {
  width: 100%;
  height: 400px;
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  backdrop-filter: blur(5px);
  overflow: hidden;
}
</style>
