import os
import random
import re
import time
import urllib.parse
import warnings

import pandas as pd
import pymssql
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 过滤警告
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

load_dotenv()

# 数据库配置
mysql_ip = os.getenv('sqlserver_ip')
mysql_port = os.getenv('sqlserver_port')
mysql_user_name = os.getenv('sqlserver_user_name')
mysql_user_pwd = os.getenv('sqlserver_user_pwd')
mysql_order_db = os.getenv('sqlserver_db')

# Doris数据库配置
doris_host = os.getenv('doris_host', '172.17.42.124')
doris_port = os.getenv('doris_port', '9030')
doris_user = os.getenv('doris_user', 'root')
doris_password = os.getenv('doris_password', 'Wjk19990921.')
doris_database = 'test'  # 固定使用test数据库

# 硅基流动API配置
SILICONFLOW_API_KEY = 'sk-vwhxpdbxdpnyozrgvbhdpoukpwasgbhscqnmlfezmwhlkkcb'
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
MODEL_NAME = "Qwen/Qwen3-8B"


def get_doris_connection():
    """获取Doris数据库连接"""
    try:
        # 使用SQLAlchemy创建连接
        engine = create_engine(
            f'mysql+pymysql://{doris_user}:{urllib.parse.quote_plus(doris_password)}@{doris_host}:{doris_port}/{doris_database}'
        )
        return engine
    except Exception as e:
        print(f"Doris连接失败: {e}")
        return None


def create_doris_table(engine):
    """在Doris中创建表 - 只包含product_name和ai_review"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS oms_order_ai_reviews (
        id BIGINT COMMENT '自增ID',
        product_name VARCHAR(500) COMMENT '产品名称',
        ai_review VARCHAR(1000) COMMENT 'AI生成的评论',
        created_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print("Doris表创建/验证成功: oms_order_ai_reviews")
        return True
    except Exception as e:
        print(f"Doris表创建失败: {e}")
        return False


def insert_data_to_doris(engine, df):
    """将数据插入到Doris - 只插入product_name和ai_review"""
    try:
        # 选择需要插入的列，只保留product_name和ai_review
        doris_df = df[['product_name', 'ai_review']].copy()

        # 添加id列（使用索引+1作为临时ID）
        doris_df['id'] = range(1, len(doris_df) + 1)

        # 插入数据到Doris
        doris_df.to_sql(
            name='oms_order_ai_reviews',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        print(f"成功插入 {len(doris_df)} 条数据到Doris")
        return True

    except Exception as e:
        print(f"插入数据到Doris失败: {e}")
        return False


def fix_encoding(text):
    """修复编码问题"""
    if not isinstance(text, str):
        return text

    encodings_to_try = [
        ('latin-1', 'utf-8'),
        ('latin-1', 'gbk'),
        ('latin-1', 'gb2312'),
        ('utf-8', 'utf-8'),
    ]

    for src_enc, dst_enc in encodings_to_try:
        try:
            return text.encode(src_enc).decode(dst_enc)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return text


def split_product_info(text):
    """根据丨和汉字拆分产品信息"""
    if not isinstance(text, str) or not text.strip():
        return {'brand': '', 'english_name': '', 'chinese_name': '', 'full_text': text}

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
            'full_text': cleaned_text
        }
    else:
        chinese_chars = re.findall(r'[\u4e00-\u9fa5]', cleaned_text)
        if chinese_chars:
            first_chinese_index = cleaned_text.find(chinese_chars[0])
            return {
                'brand': '',
                'english_name': cleaned_text[:first_chinese_index].strip(),
                'chinese_name': cleaned_text[first_chinese_index:].strip(),
                'full_text': cleaned_text
            }
        else:
            return {
                'brand': '',
                'english_name': cleaned_text,
                'chinese_name': '',
                'full_text': cleaned_text
            }


def generate_ai_review(product_info, max_retries=3):
    """
    使用硅基流动API生成商品评论
    """
    brand = product_info.get('brand', '')
    english_name = product_info.get('english_name', '')
    chinese_name = product_info.get('chinese_name', '')

    # 随机决定好评(70%)或差评(30%)
    is_positive = random.random() < 0.7

    if is_positive:
        review_type = "好评"
        sentiment_instruction = "表达满意和推荐"
    else:
        review_type = "差评"
        sentiment_instruction = "可以说脏话表达不满"

    # 构建提示词
    prompt = f"""
    请为以下商品生成一条{review_type}：
    品牌：{brand}
    产品名称：{english_name}
    产品描述：{chinese_name}

    要求：
    1. 评论长度在20-50字之间
    2. 语气自然，像真实用户写的
    3. 包含具体的使用体验和感受
    4. {sentiment_instruction}
    5. 直接返回评论内容，不要添加其他说明
    6. 不用思考
    """

    headers = {
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 100,
        "temperature": 0.8,
        "top_p": 0.8,
        "stream": False
    }

    for attempt in range(max_retries):
        try:
            print(f"  正在调用AI生成{review_type}... (尝试 {attempt + 1})")
            response = requests.post(SILICONFLOW_API_URL, headers=headers,
                                     json=payload, timeout=60)

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    review = result['choices'][0]['message']['content'].strip()
                    print(f"  {review_type}生成成功: {review}")
                    return review
                else:
                    print("  API返回格式异常")
            else:
                print(f"  API请求失败，状态码: {response.status_code}")
                if response.status_code == 429:
                    print("  达到速率限制，等待后重试...")
                    time.sleep(10)

        except requests.exceptions.Timeout:
            print("  请求超时，重试...")
        except requests.exceptions.ConnectionError:
            print("  连接错误，重试...")
        except Exception as e:
            print(f"  生成评论异常: {e}")

        # 重试前等待
        if attempt < max_retries - 1:
            wait_time = 5 * (attempt + 1)
            print(f"  等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)

    # 如果所有重试都失败，返回默认评论
    if is_positive:
        default_review = f"{brand}的{english_name}很不错，使用体验很好，值得推荐。"
    else:
        default_review = f"{brand}的{english_name}质量一般，有些失望，不太推荐。"

    print(f"  AI生成失败，使用默认{review_type}")
    return default_review


def create_new_table(conn):
    """创建新表"""
    # 先检查表是否存在
    check_table_sql = """
    IF OBJECT_ID('oms_order_dtl_enhanced2', 'U') IS NOT NULL
        DROP TABLE oms_order_dtl_enhanced2
    """

    create_table_sql = """
    CREATE TABLE oms_order_dtl_enhanced2 (
        id INT IDENTITY(1,1) PRIMARY KEY,
        order_id NVARCHAR(100),
        user_id NVARCHAR(100),
        product_id NVARCHAR(100),
        product_name NVARCHAR(500),
        brand NVARCHAR(100),
        english_name NVARCHAR(200),
        chinese_name NVARCHAR(300),
        ai_review NVARCHAR(1000),
        ds DATE,
        created_time DATETIME DEFAULT GETDATE()
    )
    """

    try:
        cursor = conn.cursor()
        # 先执行检查并删除（如果存在）
        cursor.execute(check_table_sql)
        # 再创建新表
        cursor.execute(create_table_sql)
        conn.commit()
        print("新表创建成功: oms_order_dtl_enhanced2")
    except Exception as e:
        print(f"创建表失败: {e}")


def insert_data_to_sqlserver(conn, df):
    """将数据插入到SQL Server"""
    insert_sql = """
    INSERT INTO oms_order_dtl_enhanced2
    (order_id, user_id, product_id, product_name, brand, english_name, chinese_name, ai_review, ds)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cursor = conn.cursor()
        inserted_count = 0

        for _, row in df.iterrows():
            cursor.execute(insert_sql, (
                row['order_id'],
                row['user_id'],
                row['product_id'],
                row['product_name'],
                row['brand'],
                row['english_name'],
                row['chinese_name'],
                row['ai_review'],
                row['ds']  # 新增ds字段
            ))
            inserted_count += 1

        conn.commit()
        print(f"成功插入 {inserted_count} 条数据到SQL Server")

    except Exception as e:
        print(f"插入数据失败: {e}")
        conn.rollback()


def main():
    try:
        # 连接SQL Server数据库
        conn = pymssql.connect(
            server=mysql_ip,
            user=mysql_user_name,
            password=mysql_user_pwd,
            database=mysql_order_db,
            port=int(mysql_port) if mysql_port else 1433,
            charset='UTF-8'
        )

        # 连接Doris数据库
        print("正在连接Doris数据库...")
        doris_engine = get_doris_connection()
        if doris_engine is None:
            print("Doris连接失败，退出程序")
            return

        # 创建Doris表
        if not create_doris_table(doris_engine):
            print("Doris表创建失败，退出程序")
            return

        # 创建SQL Server新表
        create_new_table(conn)

        # SQL查询 - 获取数据（包含ds字段）
        query_sql = """
        SELECT TOP 20 
            order_id,
            user_id,
            product_id,
            product_name,
            ds
        FROM oms_order_dtl;
        """

        # 获取数据
        df = pd.read_sql_query(query_sql, conn)
        print(f"获取到 {len(df)} 条原始数据")

        # 显示ds字段的分布情况
        if 'ds' in df.columns:
            ds_counts = df['ds'].value_counts().head(5)
            print(f"DS字段分布（前5个）:")
            for ds, count in ds_counts.items():
                print(f"  {ds}: {count}条")

        # 处理产品名称拆分
        print("开始处理产品信息拆分...")
        results = []
        for product_name in df['product_name']:
            result = split_product_info(product_name)
            results.append(result)

        # 合并结果
        result_df = pd.DataFrame(results)
        final_df = pd.concat([df, result_df], axis=1)

        # 为每个产品生成AI评论
        print("开始使用AI生成评论...")
        reviews = []

        for i, row in final_df.iterrows():
            print(f"\n正在处理第 {i + 1}/{len(final_df)} 条数据...")
            print(f"  产品: {row['brand']} - {row['english_name']}")
            if 'ds' in row:
                print(f"  日期: {row['ds']}")

            product_info = {
                'brand': row['brand'],
                'english_name': row['english_name'],
                'chinese_name': row['chinese_name']
            }

            # 使用AI生成评论
            review = generate_ai_review(product_info)
            reviews.append(review)

            # 添加延迟避免API限制
            print("  等待3秒...")
            time.sleep(3)

        # 添加评论列
        final_df['ai_review'] = reviews

        # 显示前5条结果
        print("\n前5条处理结果:")
        print("=" * 120)
        for i, row in final_df.head(5).iterrows():
            print(f"产品 {i + 1}:")
            print(f"  订单ID: {row['order_id']}")
            print(f"  品牌: {row['brand']}")
            print(f"  品类名称: {row['english_name']}")
            print(f"  中文描述: {row['chinese_name']}")
            if 'ds' in row:
                print(f"  日期: {row['ds']}")
            print(f"  AI评论: {row['ai_review']}")
            print("-" * 120)

        # 保存到CSV文件（备份）
        output_file = "oms_order_dtl_enhanced2_ai.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n数据已备份到CSV文件: {output_file}")

        # 插入到SQL Server新表
        print("正在将数据插入到SQL Server...")
        insert_data_to_sqlserver(conn, final_df)

        # 插入到Doris数据库 - 只插入product_name和ai_review
        print("正在将数据插入到Doris...")
        if insert_data_to_doris(doris_engine, final_df):
            print("数据成功存储到Doris的test.oms_order_ai_reviews表")
            print("Doris表字段: id, product_name, ai_review, created_time")
        else:
            print("数据存储到Doris失败")

        # 验证SQL Server插入的数据
        verify_sql = "SELECT COUNT(*) as total_count FROM oms_order_dtl_enhanced2"
        cursor = conn.cursor()
        cursor.execute(verify_sql)
        count = cursor.fetchone()[0]
        print(f"SQL Server新表中现有数据量: {count} 条")

        # 验证Doris插入的数据
        try:
            with doris_engine.connect() as doris_conn:
                doris_count = doris_conn.execute(text("SELECT COUNT(*) FROM oms_order_ai_reviews")).scalar()
                print(f"Doris表中现有数据量: {doris_count} 条")

                # 显示Doris表的前几条数据
                sample_data = doris_conn.execute(text("SELECT id, product_name, ai_review FROM oms_order_ai_reviews LIMIT 3")).fetchall()
                print("\nDoris表前3条数据:")
                for row in sample_data:
                    print(f"  ID: {row[0]}, 产品: {row[1][:30]}..., 评论: {row[2][:30]}...")

        except Exception as e:
            print(f"验证Doris数据失败: {e}")

        conn.close()

        print("\n处理完成！")
        print(f"- 原始数据: {len(df)} 条")
        print(f"- 存储位置1: SQL Server表 [oms_order_dtl_enhanced2] (完整字段)")
        print(f"- 存储位置2: Doris表 [test.oms_order_ai_reviews] (仅product_name和ai_review)")
        print(f"- 备份文件: {output_file}")

    except Exception as e:
        print(f"数据处理失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()