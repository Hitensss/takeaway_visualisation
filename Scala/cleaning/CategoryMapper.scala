package cleaning

//品类映射器
object CategoryMapper {

  def mapCategory(original: String): String = {
    if (original == null) return " 其他"

    // 快餐简餐
    if (original.contains("汉堡") || original.contains("炸鸡") ||
      original.contains("煲仔饭") || original.contains("黄焖鸡") ||
      original.contains("炒饭") || original.contains("炒面")
      || original.contains("大盘鸡") || original.contains("豆浆/油条")
      || original.contains("盖浇饭") || original.contains("干锅")
      || original.contains("锅贴/煎饺") || original.contains("其他饭类套餐")
      || original.contains("三明治") || original.contains("炸鸡")
      || original.contains("猪脚饭")) {
      return " 快餐简餐"
    }

    // 粉面粥汤
    if (original.contains("米粉/米线") || original.contains("胡辣汤") ||
      original.contains("凉皮/凉粉/擀面皮") || original.contains("馄饨") ||
      original.contains("焖面") || original.contains("螺蛳粉")
      || original.contains("面馆")|| original.contains("其他汤粉店")
      || original.contains("其他粥店")|| original.contains("砂锅粥")
      || original.contains("水饺")|| original.contains("羊汤/牛肉汤")
      || original.contains("中式拉面")|| original.contains("重庆小面")
      || original.contains("重庆小面")|| original.contains("肠粉")) {
      return " 粉面粥"
    }

    // 中餐炒菜
    if (original.contains("川菜") || original.contains("湘菜") ||
      original.contains("江西菜") || original.contains("木桶饭") ||
      original.contains("盖浇饭") || original.contains("北京菜") ||
      original.contains("家常菜") || original.contains("农家菜")
      || original.contains("本帮江浙菜")|| original.contains("东北菜")
      || original.contains("港菜")|| original.contains("贵州菜")
      || original.contains("徽菜")|| original.contains("卤味")
      || original.contains("鲁菜")|| original.contains("闽菜")
      || original.contains("排骨米饭")|| original.contains("其他地方菜")
      || original.contains("清真菜")|| original.contains("砂锅")
      || original.contains("酸菜鱼")|| original.contains("水煮鱼")
      || original.contains("台湾菜")|| original.contains("西北菜")
      || original.contains("新疆菜")|| original.contains("粤菜")
      || original.contains("蒸鸡")|| original.contains("滋补鸡")) {
      return " 中餐炒菜"
    }

    // 日韩料理
    if (original.contains("日料") || original.contains("日本料理") ||
      original.contains("寿司") || original.contains("日式") ||
      original.contains("韩式炸鸡")|| original.contains("韩国料理")
      || original.contains("烤肉饭")|| original.contains("烤肉拌饭")
      || original.contains("石锅拌饭")|| original.contains("铁板饭")
      || original.contains("板烧饭")|| original.contains("紫菜包饭")) {
      return " 日韩料理"
    }

    // 烧烤夜宵
    if (original.contains("烧烤") || original.contains("龙虾") ||
      original.contains("海鲜") || original.contains("烤鱼")
      || original.contains("羊肉馆")|| original.contains("炸串")) {
      return " 烧烤夜宵"
    }

    // 轻食健康
    if (original.contains("轻食") || original.contains("沙拉") ||
      original.contains("健康") || original.contains("咖喱饭")) {
      return " 轻食健康"
    }

    // 地方小吃
    if (original.contains("饺子") || original.contains("包子") ||
      original.contains("煎饼") || original.contains("小吃") ||
      original.contains("烧饼") || original.contains("生煎") ||
      original.contains("肠粉")|| original.contains("肉夹馍")
      || original.contains("煎饼果子")|| original.contains("手抓饼")
      || original.contains("卷饼")|| original.contains("热干面")
      || original.contains("泡馍")|| original.contains("馅饼")
      || original.contains("瓦罐")|| original.contains("炸酱面")
    ) {
      return " 地方小吃"
    }

    // 火锅冒菜
    if (original.contains("麻辣烫") || original.contains("冒菜") ||
      original.contains("麻辣香锅") || original.contains("鸡公煲") ||
      original.contains("火锅")|| original.contains("铜锅涮肉")) {
      return " 火锅冒菜"
    }

    // 甜点
    if (original.contains("牛奶") ||
      original.contains("蛋糕") || original.contains("烘焙")
      ) {
      return " 甜点"
    }
    if (original.contains("牛排") ||
      original.contains("披萨") || original.contains("西餐")
      || original.contains("意大利菜")|| original.contains("西班牙菜")
      || original.contains("墨西哥菜")
    ) {
      return " 西式料理"
    }

    " 其他"
  }
}