<template>
  <!--<dv-full-screen-container>-->
  <div class="dashboard">
    <div class="row">
      <div class="card" style="width: 15%">
        <SalesRatingScatterChart />
      </div>
      <div class="card" style="width: 18%">
        <DistanceRatingScatterChart />
      </div>
      <div class="card" style="width: 15%">
        <PriceRatingScatterChart />
      </div>
      <div class="card" style="width: 25%; margin: 0 auto">
        <CorrelationHeatmapChart />
      </div>

      <div class="card" style="width: 15%">
        <DeliveryTimeRatingScatterChart />
      </div>
      <div class="card" style="width: 15%">
        <MinPriceRatingScatterChart />
      </div>
    </div>

    <div class="row">
      <div class="card" style="width: 20%">
        <DeliveryFeeRatingScatterChart />
      </div>
      <div class="card" style="width: 20%">
        <SalesDeliveryFeeScatterChart />
      </div>
      <div class="card" style="width: 20%">
        <SalesDistanceScatterChart />
      </div>
      <div class="card" style="width: 20%">
        <DistanceRatingBoxplotChart />
      </div>
      <div class="card" style="width: 25%">
        <CategoryRatingBoxplotChart />
      </div>
    </div>
  </div>
  <!-- </dv-full-screen-container> -->
</template>

<script setup>
import SalesRatingScatterChart from '@/components/charts/SalesRatingScatterChart.vue'
import DistanceRatingScatterChart from '@/components/charts/DistanceRatingScatterChart.vue'
import DistanceRatingBoxplotChart from '@/components/charts/DistanceRatingBoxplotChart.vue'
import PriceRatingScatterChart from '@/components/charts/PriceRatingScatterChart.vue'
import CategoryRatingBoxplotChart from '@/components/charts/CategoryRatingBoxplotChart.vue'
import DeliveryFeeRatingScatterChart from '@/components/charts/DeliveryFeeRatingScatterChart.vue'
import DeliveryTimeRatingScatterChart from '@/components/charts/DeliveryTimeRatingScatterChart.vue'
import MinPriceRatingScatterChart from '@/components/charts/MinPriceRatingScatterChart.vue'
import SalesDeliveryFeeScatterChart from '@/components/charts/SalesDeliveryFeeScatterChart.vue'
import SalesDistanceScatterChart from '@/components/charts/SalesDistanceScatterChart.vue'
import CorrelationHeatmapChart from '@/components/charts/CorrelationHeatmapChart.vue'
</script>

<style scoped>
.dashboard {
  width: 100%;
  overflow-y: hidden;
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
