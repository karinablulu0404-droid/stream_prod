import os
import re
import warnings

import pandas as pd
import pymssql
from dotenv import load_dotenv

# 过滤警告
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

load_dotenv()

# 获取环境变量并添加验证
mysql_ip = os.getenv('sqlserver_ip')
mysql_port = os.getenv('sqlserver_port')
mysql_user_name = os.getenv('sqlserver_user_name')
mysql_user_pwd = os.getenv('sqlserver_user_pwd')
mysql_order_db = os.getenv('sqlserver_db')

# 验证环境变量
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

    # 验证端口，如果未设置则使用默认值
    if not mysql_port:
        print("警告: sqlserver_port 未设置，使用默认端口 1433")

    return True

def fix_encoding(text):
    """
    修复编码问题
    """
    if not isinstance(text, str):
        return text

    # 尝试不同的编码修复方式
    encodings_to_try = [
        ('latin-1', 'utf-8'),
        ('latin-1', 'gbk'),
        ('latin-1', 'gb2312'),
        ('utf-8', 'utf-8'),
        ('gbk', 'utf-8')
    ]

    for src_enc, dst_enc in encodings_to_try:
        try:
            # 先编码再解码
            return text.encode(src_enc).decode(dst_enc)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue

    # 如果所有编码都失败，返回原文本
    return text


def split_product_info(text):
    """
    根据丨和汉字拆分产品信息
    """
    if not isinstance(text, str) or not text.strip():
        return {
            'brand': '',
            'english_name': '',
            'chinese_name': '',
            'full_text': text
        }

    # 先修复编码
    cleaned_text = fix_encoding(text)

    # 使用正则表达式匹配
    # 匹配模式：品牌丨英文部分 中文部分 其他信息
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
        # 如果没有匹配到，尝试其他拆分方式
        # 检查是否包含中文
        chinese_chars = re.findall(r'[\u4e00-\u9fa5]', cleaned_text)
        if chinese_chars:
            # 找到第一个汉字的位置
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


try:
    # 验证环境变量
    if not validate_env_variables():
        exit(1)

    # 设置端口，如果为空则使用默认值
    port = int(mysql_port) if mysql_port else 1433

    # 连接数据库
    print(f"尝试连接数据库: {mysql_ip}:{port}, 数据库: {mysql_order_db}")
    conn = pymssql.connect(
        server=mysql_ip,
        user=mysql_user_name,
        password=mysql_user_pwd,
        database=mysql_order_db,
        port=port,
        charset='UTF-8'
    )
    print("数据库连接成功!")

    # SQL查询
    query_sql = """
    SELECT 
        order_id,
        user_id,
        product_id,
        total_amount,
        product_name
    FROM oms_order_dtl;
    """

    # 获取数据
    df = pd.read_sql_query(query_sql, conn)
    print(f"获取到 {len(df)} 条数据")

    # 对product_name列应用编码修复
    df['product_name_cleaned'] = df['product_name'].apply(fix_encoding)

    # 处理所有产品名称
    results = []
    for i, row in df.iterrows():
        product_name = row['product_name']
        result = split_product_info(product_name)
        results.append(result)

    # 将结果合并到原始DataFrame
    result_df = pd.DataFrame(results)
    final_df = pd.concat([df, result_df], axis=1)

    # 打印前10条结果
    print("\n前10条产品信息拆分结果:")
    print("=" * 100)
    for i, row in final_df.head(10).iterrows():
        print(f"产品 {i + 1}:")
        print(f"  订单ID: {row['order_id']}")
        print(f"  品牌: {row['brand']}")
        print(f"  英文部分: {row['english_name']}")
        print(f"  中文部分: {row['chinese_name']}")
        print(f"  修复后完整文本: {row['full_text']}")
        print("-" * 100)

    # 统计品牌分布
    brand_stats = final_df['brand'].value_counts()
    print("\n品牌统计:")
    for brand, count in brand_stats.items():
        print(f"  {brand}: {count}条")

    conn.close()

except Exception as e:
    print(f"数据处理失败: {e}")
    import traceback
    traceback.print_exc()