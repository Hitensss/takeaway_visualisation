package model
//数据模型
case class CleanedData(
                        shop_name: String,
                        category_raw: String,
                        category_clean: String,
                        monthly_sales: Int,
                        rating: Double,
                        distance: Int,
                        delivery_time: Int,
                        delivery_fee: Double,
                        min_price: Double,
                        avg_price: Double
                      )