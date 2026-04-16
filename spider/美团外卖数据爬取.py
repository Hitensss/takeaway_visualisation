# API Docs: https://reqable.com/docs/capture/addons

from reqable import *
import json
import re
import win32com.client as win32

def onRequest(context, request):
  # Print url to console
  # print('request url ' + context.url)

  # Update or add a query parameter
  # request.queries['foo'] = 'bar'

  # Update or add a http header
  # request.headers['foo'] = 'bar'

  # Replace http body with a text
  # request.body = 'Hello World'

  # Map with a local file
  # request.body.file('~/Desktop/body.json')

  # Convert to dict if the body is a JSON
  # request.body.jsonify()
  # Update the JSON content
  # request.body['foo'] = 'bar'

  # Done
  return request

def onResponse(context, response):
  # Update status code
  # response.code = 404

  # APIs are same as `onRequest`

  # Done
  
  # 将响应体转换为JSON对象
  response.body.jsonify()
  # 提取商家列表数据
  poiList = response.body['data']['poilist']

  all_poi_list = []
  
    # 慢速处理：每条数据处理时添加延迟
  for index, poi in enumerate(poiList):
    # 添加随机延迟，模拟人工操作
    if index > 0 and index % 5 == 0:  # 每5条数据休息一下
      sleep_time = random.uniform(0.5, 1.5)  # 随机休息0.5-1.5秒
      print(f"已处理{index}条，休息{sleep_time:.1f}秒...")
      time.sleep(sleep_time)
  
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
      exc.Visible = True  # 设置exc应用程序窗口可见
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
    
      # 从下一行开始追加数据
      for row_data in all_poi_list:
          for col, key in enumerate(['商家ID', '商家名称', '状态', '月售', '评分', '距离', '送达时间', '配送费', '起送价', '人均', '品类', '打烊提示'], start=1):
              value = row_data.get(key)
              sheet.Cells(last_row, col).value = value
          last_row = last_row + 1
    
      # 保存工作薄
      workbook.Save()
      print('商家数据追加成功！')

  except Exception as e:
      print(f'发生错误：{e}')
      print('请检查：\n1. Excel文件路径是否正确\n2. Excel文件是否已关闭\n3. 是否有写入权限')
  return response
