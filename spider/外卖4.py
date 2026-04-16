# API Docs: https://reqable.com/docs/capture/addons

from reqable import *
import json
import re
import win32com.client as win32
import time
import random

def onRequest(context, request):
  global request_counter
  request_counter += 1
  print(f"\n=== 开始处理请求 #{request_counter} ===")
  
  # 1. 随机延迟（4-8秒）- 防封第一道防线
  delay = random.uniform(4, 8)
  print(f"⏳ 延迟 {delay:.1f} 秒...")
  time.sleep(delay)
  # 2. 随机User-Agent
  ua_list = [
    "Mozilla/5.0 (Linux; Android 13; SM-S908E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.143 Mobile Safari/537.36",
     "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
    ]
    request.headers['User-Agent'] = random.choice(ua_list)
    
    # 3. 每10次请求长休息
    if request_counter % 10 == 0:
        long_break = random.uniform(30, 60)
        print(f"☕ 已完成{request_counter}次请求，休息{long_break:.0f}秒...")
        time.sleep(long_break)
  return request

def onResponse(context, response):
  # 检查是否被封
  if response.code in [403, 429]:
    print("\n" + "="*50)
    print("🚨 检测到IP可能被封！")
    print("📋 请手动更换代理IP")
    print("\007")  # 响铃提醒
  # 将响应体转换为JSON对象
  response.body.jsonify()
  # 提取商家列表数据
  poiList = response.body['data']['poilist']

  all_poi_list = []
  
    # 慢速处理：每条数据处理时添加延迟
  for index, poi in enumerate(poiList):
    #添加随机延迟，模拟人工操作
    base_delay = random.uniform(4, 8)
    time.sleep(base_delay)
     if index > 0 and index % 10 == 0:  # 每10条数据休息一下
      long_break = random.uniform(5,12 ) # 休息()秒
      print(f"休息{long_break:.1f}秒...")
      time.sleep(long_break)
      
  
  for poi in poiList:
      poi_id = poi.get('id', '')
      poi_name = poi.get('name', '')
      poi_status = poi.get('status', '')
      month_sales = poi.get('month_sales_tip', '')
      poi_score = poi.get('wm_poi_score', '')
      distance = poi.get('distance', '')
      delivery_time = poi.get('delivery_time_tip', '')
      shipping_fee = poi.get('shipping_fee_tip', '')
      min_price = poi.get('min_price_tip', '')
      avg_price = poi.get('average_price_tip', '')
      category = poi.get('third_category', '')
      closing_tips = poi.get('closing_tips', '')
    
      info = {
          '商家ID': poi_id,
          '商家名称': poi_name,
          '状态': poi_status,
          '月售': month_sales,
          '评分': poi_score,
          '距离': distance,
          '送达时间': delivery_time,
          '配送费': shipping_fee,
          '起送价': min_price,
          '人均': avg_price,
          '品类': category,
          '打烊提示': closing_tips
      }
      all_poi_list.append(info)

  # 打印数据查看
  print(json.dumps(all_poi_list, ensure_ascii=False, indent=2))

  all_poi_str = json.dumps(all_poi_list, ensure_ascii=False)

  # 提取数据列表
  match = re.search(r'\[.*\]', all_poi_str)
  if match:
      all_poi_list = eval(match.group())
  else:
      all_poi_list = []

  # 定义Excel文件路径
  file_path = r"E:\study\毕业论文\spider\美团外卖商家数据集.xlsx"

  try:
      # 尝试使用Excel.Application
      exc = win32.Dispatch('Excel.Application')
      exc.Visible = True  # 设置为不可见或不可见
      # 打开指定路径的工作薄
      workbook = exc.Workbooks.Open(file_path)
      # 选择活动工作表
      sheet = workbook.ActiveSheet
      # 获取当前工作表已使用区域的真实最后一行
      if sheet.Cells(1, 1).value is None:
          last_row = 1
      else:
          last_row = sheet.Cells(sheet.Rows.Count, 1).End(-4162).Row + 1
      print(f'即将从第{last_row}行开始写入数据')

      # 写入表头
      if sheet.Cells(1, 1).value is None:
          headers = ['商家ID', '商家名称', '状态', '月售', '评分', '距离', '送达时间', '配送费', '起送价', '人均', '品类', '打烊提示']
          for col, header in enumerate(headers, start=1):
              sheet.Cells(1, col).value = header
          last_row = 2  # 如果写入了表头，从第二行开始写入数据
      
          # 慢速写入Excel（每写一条休息一下）
      success_count = 0
      for row_data in all_poi_list:
        # 写入前随机延迟
        time.sleep(random.uniform(0.3, 0.8))
      
      # 从下一行开始追加数据
      for row_data in all_poi_list:
          for col, key in enumerate(['商家ID', '商家名称', '状态', '月售', '评分', '距离', '送达时间', '配送费', '起送价', '人均', '品类', '打烊提示'], start=1):
              value = row_data.get(key)
              sheet.Cells(last_row, col).value = value
          last_row = last_row + 1
          success_count += 1
          
          # 每10条保存一次，避免数据丢失
          if success_count % 10 == 0:
            workbook.Save()
            print(f"已保存 {success_count} 条数据...")
    
      # 最终保存
      workbook.Save()
      print('商家数据追加成功！')

  except Exception as e:
      print(f'发生错误：{e}')
      print('请检查：\n1. Excel文件路径是否正确\n2. Excel文件是否已关闭\n3. 是否有写入权限')
  return response
