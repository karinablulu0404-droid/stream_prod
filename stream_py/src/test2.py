import os
import re
import time
import warnings
from datetime import datetime

import pandas as pd
import pymssql
from dotenv import load_dotenv

# 过滤警告
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

load_dotenv()

# 获取环境变量
mysql_ip = os.getenv('sqlserver_ip')
mysql_port = os.getenv('sqlserver_port')
mysql_user_name = os.getenv('sqlserver_user_name')
mysql_user_pwd = os.getenv('sqlserver_user_pwd')
mysql_order_db = os.getenv('sqlserver_db')

# 验证环境变量函数（保持不变）
def validate_env_variables():
    required_vars = {
        'sqlserver_ip': mysql_ip,
        'sqlserver_user_name': mysql_user_name,
        'sqlserver_user_pwd': mysql_user_pwd,
        'sqlserver_db': mysql_order_db
    }

    missing_vars = []
    for var_name, var_value in required_vars.items():
        if not var_value:
            missing_vars.append(var_name)

    if missing_vars:
        print(f"错误: 以下环境变量缺失或为空: {', '.join(missing_vars)}")
        print("请检查您的 .env 文件是否包含这些变量:")
        for var in missing_vars:
            print(f"  {var}=您的值")
        return False

    if not mysql_port:
        print("警告: sqlserver_port 未设置，使用默认端口 1433")
    return True

# 编码修复函数（保持不变）
def fix_encoding(text):
    if not isinstance(text, str):
        return text

    encodings_to_try = [
        ('latin-1', 'utf-8'),
        ('latin-1', 'gbk'),
        ('latin-1', 'gb2312'),
        ('utf-8', 'utf-8'),
        ('gbk', 'utf-8')
    ]

    for src_enc, dst_enc in encodings_to_try:
        try:
            return text.encode(src_enc).decode(dst_enc)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return text

# 产品信息拆分函数（保持不变）
def split_product_info(text):
    if not isinstance(text, str) or not text.strip():
        return {
            'brand': '',
            'english_name': '',
            'chinese_name': '',
            'full_text': text
        }

    cleaned_text = fix_encoding(text)
    pattern = r'^([^丨]+)丨([^\u4e00-\u9fa5]*)([\u4e00-\u9fa5].*)$'

    match = re.match(pattern, cleaned_text)
    if match:
        brand = match.group(1).strip()
        english_part = match.group(2).strip()
        chinese_part = match.group(3).strip()

        return {
            'brand': brand,
            'english_name': english_part,
            'chinese_name': chinese_part,
            'full_text': cleaned_text,
            'original_text': text
        }
    else:
        chinese_chars = re.findall(r'[\u4e00-\u9fa5]', cleaned_text)
        if chinese_chars:
            first_chinese_index = cleaned_text.find(chinese_chars[0])
            return {
                'brand': '',
                'english_name': cleaned_text[:first_chinese_index].strip(),
                'chinese_name': cleaned_text[first_chinese_index:].strip(),
                'full_text': cleaned_text,
                'original_text': text
            }
        else:
            return {
                'brand': '',
                'english_name': cleaned_text,
                'chinese_name': '',
                'full_text': cleaned_text,
                'original_text': text
            }

def get_database_connection():
    """获取数据库连接"""
    port = int(mysql_port) if mysql_port else 1433
    return pymssql.connect(
        server=mysql_ip,
        user=mysql_user_name,
        password=mysql_user_pwd,
        database=mysql_order_db,
        port=port,
        charset='UTF-8'
    )

def process_new_data(last_timestamp=None):
    """处理新数据 - 使用时间戳字段"""
    try:
        conn = get_database_connection()

        # 根据时间戳来查询新数据
        if last_timestamp:
            query_sql = """
            SELECT 
                order_id,
                user_id,
                product_id,
                total_amount,
                product_name,
                ds,
                ts
            FROM oms_order_dtl 
            WHERE ts > %s
            ORDER BY ts;
            """
            df = pd.read_sql_query(query_sql, conn, params=[last_timestamp])
        else:
            # 第一次运行，获取所有数据
            query_sql = """
            SELECT 
                order_id,
                user_id,
                product_id,
                total_amount,
                product_name,
                ds,
                ts
            FROM oms_order_dtl
            ORDER BY ts;
            """
            df = pd.read_sql_query(query_sql, conn)

        conn.close()

        if len(df) == 0:
            return None, last_timestamp

        # 处理产品名称
        df['product_name_cleaned'] = df['product_name'].apply(fix_encoding)

        results = []
        for i, row in df.iterrows():
            product_name = row['product_name']
            result = split_product_info(product_name)
            results.append(result)

        # 合并结果
        result_df = pd.DataFrame(results)
        final_df = pd.concat([df, result_df], axis=1)

        # 获取最新的时间戳
        new_last_timestamp = final_df['ts'].max()

        return final_df, new_last_timestamp

    except Exception as e:
        print(f"数据处理失败: {e}")
        return None, last_timestamp

def real_time_monitoring():
    """实时监控主函数"""
    print("=== 开始实时数据监控 ===")
    print("按 Ctrl+C 停止监控")

    # 验证环境变量
    if not validate_env_variables():
        return

    last_timestamp = None
    check_interval = 30  # 检查间隔（秒）

    try:
        while True:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{current_time}] 检查新数据...")

            # 处理新数据
            new_data, last_timestamp = process_new_data(last_timestamp)

            if new_data is not None and len(new_data) > 0:
                print(f"发现 {len(new_data)} 条新记录:")
                print("=" * 80)

                for i, row in new_data.iterrows():
                    print(f"新订单 {i + 1}:")
                    print(f"  订单ID: {row['order_id']}")
                    print(f"  用户ID: {row['user_id']}")
                    print(f"  产品ID: {row['product_id']}")
                    print(f"  品牌: {row['brand']}")
                    print(f"  英文部分: {row['english_name']}")
                    print(f"  中文部分: {row['chinese_name']}")
                    print(f"  金额: {row['total_amount']}")
                    print(f"  时间: {row['ds']}")
                    print("-" * 80)

                # 品牌统计
                brand_stats = new_data['brand'].value_counts()
                print("新数据品牌统计:")
                for brand, count in brand_stats.items():
                    print(f"  {brand}: {count}条")
            else:
                print("没有发现新数据")

            print(f"等待 {check_interval} 秒后再次检查...")
            time.sleep(check_interval)

    except KeyboardInterrupt:
        print("\n\n=== 监控已停止 ===")
    except Exception as e:
        print(f"监控出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    real_time_monitoring()