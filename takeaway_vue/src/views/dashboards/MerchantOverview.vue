<template>
  <!--<dv-full-screen-container>-->

  <div class="dashboard">
    <!-- 第一行 -->
    <div class="row">
      <div class="card" style="width: 30%">
        <CategoryRatingStatsCard />
      </div>

      <div class="card" style="width: 25%">
        <CategoryRatingDistributionChart />
      </div>
      <div class="card" style="width: 20%">
        <RatingDistributionChart />
      </div>

      <div class="card" style="width: 35%">
        <CategoryStatsChart />
      </div>
      <div class="card" style="width: 18%">
        <SalesSummaryCard />
      </div>
    </div>

    <!-- 第二行 -->
    <div class="row">
      <div class="card" style="width: 20%">
        <SalesDistributionChart />
      </div>

      <div class="card" style="width: 20%">
        <PriceDistributionChart />
      </div>
      <div class="card" style="width: 25%">
        <CategorySalesGroupChart />
      </div>

      <div class="card" style="width: 20%">
        <PriceRatingGroupChart />
      </div>
      <div class="card" style="width: 20%">
        <CategoryWordcloudChart />
      </div>
    </div>
  </div>
  <!-- </dv-full-screen-container> -->
</template>

<script setup>
import CategoryRatingStatsCard from '@/components/charts/CategoryRatingStatsCard.vue'
import RatingDistributionChart from '@/components/charts/RatingDistributionChart.vue'
import SalesSummaryCard from '@/components/charts/SalesSummaryCard.vue'
import CategoryStatsChart from '@/components/charts/CategoryStatsChart.vue'
import CategoryRatingDistributionChart from '@/components/charts/CategoryRatingDistributionChart.vue'
import CategoryWordcloudChart from '@/components/charts/CategoryWordcloudChart.vue'
import PriceDistributionChart from '@/components/charts/PriceDistributionChart.vue'
import PriceRatingGroupChart from '@/components/charts/PriceRatingGroupChart.vue'
import SalesDistributionChart from '@/components/charts/SalesDistributionChart.vue'
import CategorySalesGroupChart from '@/components/charts/CategorySalesGroupChart.vue'
import TopShopsBarChart from '@/components/charts/TopShopsBarChart.vue'
</script>

<style scoped>
.dashboard {
  width: 100%;
}
.row {
  display: flex;
  gap: 10px;
  margin-bottom: 10px;
  flex-wrap: nowrap;
}
.card {
  width: 100%;
  height: 450px;
  background: rgba(6, 30, 55, 0.4);
  border-radius: 12px;
  backdrop-filter: blur(5px);
  overflow: hidden;
}
</style>
