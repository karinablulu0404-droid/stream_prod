import logging
import os
import random
import re
import time
import warnings
from typing import Dict, List, Set

import pandas as pd
import pymssql
import requests
from dotenv import load_dotenv

# 过滤警告
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# 数据库配置
mysql_ip = os.getenv('sqlserver_ip')
mysql_port = os.getenv('sqlserver_port')
mysql_user_name = os.getenv('sqlserver_user_name')
mysql_user_pwd = os.getenv('sqlserver_user_pwd')
mysql_order_db = os.getenv('sqlserver_db')

# 硅基流动API配置
SILICONFLOW_API_KEY = 'sk-vwhxpdbxdpnyozrgvbhdpoukpwasgbhscqnmlfezmwhlkkcb'
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
MODEL_NAME = "Qwen/Qwen3-8B"

class SensitiveWordClassifier:
    """敏感词分类器 - 从111.py集成"""

    def __init__(self, use_enhanced_matching: bool = True):
        self.p0_words: Set[str] = set()  # 严重违规
        self.p1_words: Set[str] = set()  # 脏话、对线语句、地域歧视、地狱笑话
        self.p2_words: Set[str] = set()  # 一般敏感词
        self._word_categories: Dict[str, str] = {}  # 词到分类的映射
        self.use_enhanced_matching = use_enhanced_matching

        # 基础敏感词库（确保覆盖常见敏感词）
        self.base_sensitive_words = {
            "p0": {"毒品", "枪支", "爆炸", "恐怖", "分裂", "邪教", "诈骗", "赌博", "色情"},
            "p1": {"傻逼", "操你", "妈的", "尼玛", "草泥马", "法克", "废物", "垃圾", "蠢货", "白痴"},
            "p2": {"地域黑", "地图炮", "死全家", "去死", "白皮猪", "黑鬼"}
        }

    def load_sensitive_words(self, custom_file_path: str = None) -> bool:
        """加载并分类敏感词"""
        try:
            file_path = self._find_sensitive_words_file(custom_file_path)
            file_words = []

            if file_path:
                file_words = self._read_words_from_file(file_path)
                logger.info(f"从文件加载了 {len(file_words)} 个敏感词")

            # 合并基础词库和文件词库
            all_words = self._merge_word_lists(file_words)
            self._classify_words(all_words)

            logger.info(f"敏感词分类完成 - P0: {len(self.p0_words)}个, P1: {len(self.p1_words)}个, P2: {len(self.p2_words)}个")
            return True

        except Exception as e:
            logger.error(f"加载失败: {e}，使用基础词库")
            self._load_base_words_only()
            return False

    def _merge_word_lists(self, file_words: List[str]) -> List[str]:
        """合并词库"""
        all_words = set()
        for category_words in self.base_sensitive_words.values():
            all_words.update(category_words)
        all_words.update(file_words)
        logger.info(f"合并后总词数: {len(all_words)}")
        return list(all_words)

    def _load_base_words_only(self):
        """仅加载基础词库"""
        all_words = set()
        for category_words in self.base_sensitive_words.values():
            all_words.update(category_words)
        self._classify_words(list(all_words))

    def _find_sensitive_words_file(self, custom_path: str = None) -> str:
        """查找敏感词文件"""
        possible_paths = [
            custom_path,
            r"D:\sx1\stream_prod\stream_py\src\test\Identify-sensitive-words.txt",
            "D:/sx1/stream_prod/stream_py/src/test/Identify-sensitive-words.txt",
            "./Identify-sensitive-words.txt",
            "Identify-sensitive-words.txt",
        ]
        for file_path in possible_paths:
            if file_path and os.path.exists(file_path):
                logger.info(f"找到敏感词文件: {file_path}")
                return file_path
        logger.warning("未找到敏感词文件")
        return None

    def _read_words_from_file(self, file_path: str) -> List[str]:
        """从文件读取敏感词"""
        words = set()
        try:
            with open(file_path, "r", encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word and not word.startswith('#'):
                        if '|' in word:
                            words.update(w.strip() for w in word.split('|') if w.strip())
                        else:
                            words.add(word)
            return list(words)
        except Exception as e:
            logger.error(f"读取文件失败: {e}")
            return []

    def _classify_words(self, all_words: List[str]):
        """将敏感词分类"""
        for word in all_words:
            category = self._determine_category(word)
            if category == "p0":
                self.p0_words.add(word)
            elif category == "p1":
                self.p1_words.add(word)
            else:
                self.p2_words.add(word)
            self._word_categories[word] = category

    def _determine_category(self, word: str) -> str:
        """确定敏感词分类"""
        # 优先检查基础词库
        if word in self.base_sensitive_words["p0"]:
            return "p0"
        elif word in self.base_sensitive_words["p1"]:
            return "p1"
        elif word in self.base_sensitive_words["p2"]:
            return "p2"

        # 基于关键词分类
        p0_keywords = ['毒品', '枪支', '暴恐', '分裂', '邪教', '诈骗', '爆炸', '恐怖', '赌博', '色情']
        p1_keywords = ['傻逼', '操你', '妈的', '地域黑', '地图炮', '尼哥', '尼玛', '草泥马', '法克', '黑鬼', '白皮猪']

        if any(keyword in word for keyword in p0_keywords):
            return "p0"
        elif any(keyword in word for keyword in p1_keywords):
            return "p1"
        elif len(word) <= 3:
            return "p1"
        else:
            return "p2"

    def check_sensitive(self, text: str) -> Dict[str, List[str]]:
        """检测文本中是否包含敏感词"""
        if not text or not isinstance(text, str):
            return {"p0": [], "p1": [], "p2": []}

        result = {"p0": [], "p1": [], "p2": []}
        text_lower = text.lower()

        if self.use_enhanced_matching:
            # 增强匹配：直接匹配 + 分词匹配
            found_words = set()

            # 直接匹配
            for word, category in self._word_categories.items():
                if word in text or word.lower() in text_lower:
                    found_words.add((word, category))

            # 简单分词匹配
            segments = self._simple_segment(text)
            for segment in segments:
                for word, category in self._word_categories.items():
                    if word in segment or word.lower() in segment.lower():
                        found_words.add((word, category))

            # 整理结果
            for word, category in found_words:
                result[category].append(word)
        else:
            # 普通匹配
            for word, category in self._word_categories.items():
                if word in text or word.lower() in text_lower:
                    result[category].append(word)

        # 去重
        for category in result:
            result[category] = list(set(result[category]))

        return result

    def _simple_segment(self, text: str) -> List[str]:
        """简单分词"""
        delimiters = '，。！？；：""''（）【】《》〈〉、\n\t '
        pattern = f'([{re.escape(delimiters)}])'
        segments = re.split(pattern, text)
        return [seg for seg in segments if seg.strip()]

    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            "p0_count": len(self.p0_words),
            "p1_count": len(self.p1_words),
            "p2_count": len(self.p2_words),
            "total_count": len(self._word_categories)
        }

# 初始化敏感词分类器
classifier = SensitiveWordClassifier(use_enhanced_matching=True)
classifier.load_sensitive_words()

def get_random_element(lst):
    """从列表中随机返回一个元素"""
    if not lst:
        return None
    return random.choice(lst)

def detect_sensitive_content(text):
    """检测文本中的敏感内容 - 使用新的分类器"""
    if not isinstance(text, str):
        return False, []

    result = classifier.check_sensitive(text)

    # 合并所有级别的敏感词
    all_detected_words = []
    for category in ["p0", "p1", "p2"]:
        all_detected_words.extend(result[category])

    has_sensitive = len(all_detected_words) > 0
    return has_sensitive, all_detected_words

def mark_violation_user(conn, user_id, order_id, sensitive_words, review_content):
    """标记违规用户并记录到数据库 - 增强版本，包含敏感词级别"""
    try:
        cursor = conn.cursor()

        # 检查用户违规表是否存在，不存在则创建（包含所有必要字段）
        check_table_sql = """
        IF OBJECT_ID('user_violation_records', 'U') IS NULL
        BEGIN
            CREATE TABLE user_violation_records (
                id INT IDENTITY(1,1) PRIMARY KEY,
                user_id NVARCHAR(100),
                order_id NVARCHAR(100),
                sensitive_words NVARCHAR(500),
                review_content NVARCHAR(1000),
                violation_level NVARCHAR(50),
                p0_count INT DEFAULT 0,
                p1_count INT DEFAULT 0,
                p2_count INT DEFAULT 0,
                handled BIT DEFAULT 0,
                created_time DATETIME DEFAULT GETDATE()
            )
            PRINT '用户违规记录表创建成功: user_violation_records'
        END
        ELSE
        BEGIN
            -- 表已存在，检查并添加缺失字段
            IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('user_violation_records') AND name = 'p0_count')
            BEGIN
                ALTER TABLE user_violation_records ADD p0_count INT DEFAULT 0
                PRINT '添加字段: p0_count'
            END
            
            IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('user_violation_records') AND name = 'p1_count')
            BEGIN
                ALTER TABLE user_violation_records ADD p1_count INT DEFAULT 0
                PRINT '添加字段: p1_count'
            END
            
            IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('user_violation_records') AND name = 'p2_count')
            BEGIN
                ALTER TABLE user_violation_records ADD p2_count INT DEFAULT 0
                PRINT '添加字段: p2_count'
            END
            
            IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('user_violation_records') AND name = 'violation_level')
            BEGIN
                ALTER TABLE user_violation_records ADD violation_level NVARCHAR(50)
                PRINT '添加字段: violation_level'
            END
        END
        """
        cursor.execute(check_table_sql)
        conn.commit()  # 提交表结构变更

        # 分析敏感词级别
        p0_count = 0
        p1_count = 0
        p2_count = 0

        for word in sensitive_words:
            category = classifier._word_categories.get(word, "p2")
            if category == "p0":
                p0_count += 1
            elif category == "p1":
                p1_count += 1
            else:
                p2_count += 1

        # 确定违规级别
        violation_level = "LOW"
        if p0_count > 0:
            violation_level = "CRITICAL"
        elif p1_count >= 2:
            violation_level = "HIGH"
        elif p1_count >= 1 or p2_count >= 3:
            violation_level = "MEDIUM"

        # 插入违规记录
        insert_sql = """
        INSERT INTO user_violation_records 
        (user_id, order_id, sensitive_words, review_content, violation_level, p0_count, p1_count, p2_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(insert_sql, (
            user_id,
            order_id,
            ','.join(sensitive_words),
            review_content,
            violation_level,
            p0_count,
            p1_count,
            p2_count
        ))

        conn.commit()
        logger.warning(f"用户 {user_id} 因使用敏感词被标记为违规，级别: {violation_level}")
        return True

    except Exception as e:
        logger.error(f"标记违规用户失败: {e}")
        conn.rollback()
        return False

def process_violation_users(conn):
    """处理违规用户（这里可以扩展为发送警告、限制权限等）"""
    try:
        cursor = conn.cursor()

        # 获取未处理的高风险违规用户
        query_sql = """
        SELECT user_id, violation_level, sensitive_words, p0_count, p1_count, p2_count, created_time
        FROM user_violation_records 
        WHERE handled = 0 AND violation_level IN ('CRITICAL', 'HIGH', 'MEDIUM')
        ORDER BY created_time DESC
        """

        cursor.execute(query_sql)
        violations = cursor.fetchall()

        for violation in violations:
            user_id, level, words, p0_count, p1_count, p2_count, create_time = violation
            logger.info(f"处理违规用户: {user_id}, 级别: {level}, P0: {p0_count}, P1: {p1_count}, P2: {p2_count}, 敏感词: {words}")

            # 这里可以添加具体的处理逻辑，比如：
            # 1. 发送警告通知
            # 2. 限制用户评论权限
            # 3. 记录到用户黑名单
            # 4. 通知管理员

            # 标记为已处理
            update_sql = "UPDATE user_violation_records SET handled = 1 WHERE user_id = %s AND handled = 0"
            cursor.execute(update_sql, (user_id,))

        conn.commit()
        logger.info(f"已处理 {len(violations)} 个违规用户")

    except Exception as e:
        logger.error(f"处理违规用户失败: {e}")
        conn.rollback()

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

def generate_ai_review(product_info, user_id, order_id, conn, max_retries=3):
    """
    使用硅基流动API生成商品评论，加入语言和脏话过滤，并检测违规内容
    """
    brand = product_info.get('brand', '')
    english_name = product_info.get('english_name', '')
    chinese_name = product_info.get('chinese_name', '')

    # 随机决定评论类型：55%好评，30%中评，15%差评
    random_num = random.random()

    if random_num < 0.55:
        # 好评 55%
        review_type = "好评"
        sentiment_instruction = "表达满意和推荐，语气积极正面"
    elif random_num < 0.85:
        # 中评 30%
        review_type = "中评"
        sentiment_instruction = "表达中立客观，既有优点也有缺点，语气平和"
    else:
        # 差评 15%
        review_type = "差评"
        sentiment_instruction = "表达不满和批评，语气可以强烈一些"

    # 脏话后缀列表（从py2.py中集成）
    sensitive_suffixes = [
        " 妈的",
        " 操",
        " 傻逼",
        " 垃圾",
        " 特么的",
        " 草",
        " 日",
        " 靠",
        " 尼玛",
        " 我操",
        " 真他妈",
        " 狗日的",
        " 滚蛋",
        " 去死",
        " 王八蛋"
    ]

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
            logger.info(f"正在调用AI生成{review_type}... (尝试 {attempt + 1})")
            response = requests.post(SILICONFLOW_API_URL, headers=headers,
                                     json=payload, timeout=60)

            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    review = result['choices'][0]['message']['content'].strip()

                    # 使用新的敏感词检测方法
                    has_sensitive, detected_words = detect_sensitive_content(review)
                    if has_sensitive:
                        # 标记违规用户
                        mark_violation_user(conn, user_id, order_id, detected_words, review)
                        # 替换敏感词
                        for word in detected_words:
                            review = review.replace(word, "***")
                        logger.warning(f"检测到敏感词并已处理: {detected_words}")

                    # 为差评添加敏感词后缀（30%概率，从py2.py中的0.3概率）
                    if review_type == "差评" and random.random() < 0.3:
                        suffix = get_random_element(sensitive_suffixes)
                        if suffix:
                            review += suffix
                            # 检测添加的后缀是否包含敏感词
                            has_sensitive_suffix, suffix_words = detect_sensitive_content(suffix)
                            if has_sensitive_suffix:
                                mark_violation_user(conn, user_id, order_id, suffix_words, review)
                                logger.warning(f"后缀包含敏感词，用户已被标记: {suffix_words}")
                            logger.info(f"  {review_type}生成成功（含敏感后缀）: {review}")
                    else:
                        logger.info(f"  {review_type}生成成功: {review}")

                    return review
                else:
                    logger.error("API返回格式异常")
            else:
                logger.error(f"API请求失败，状态码: {response.status_code}")
                if response.status_code == 429:
                    logger.warning("达到速率限制，等待后重试...")
                    time.sleep(10)

        except requests.exceptions.Timeout:
            logger.error("请求超时，重试...")
        except requests.exceptions.ConnectionError:
            logger.error("连接错误，重试...")
        except Exception as e:
            logger.error(f"生成评论异常: {e}")

        # 重试前等待
        if attempt < max_retries - 1:
            wait_time = 5 * (attempt + 1)
            logger.info(f"等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)

    # 如果所有重试都失败，返回默认评论
    if random_num < 0.55:
        default_review = f"{brand}的{english_name}很不错，使用体验很好，值得推荐。"
    elif random_num < 0.85:
        default_review = f"{brand}的{english_name}整体还可以，有些地方不错，但也有一些小问题。"
    else:
        default_review = f"{brand}的{english_name}质量一般，有些失望，不太推荐。"
        # 为差评默认评论添加敏感词后缀（30%概率）
        if random.random() < 0.3:
            suffix = get_random_element(sensitive_suffixes)
            if suffix:
                default_review += suffix
                # 检测默认评论的后缀
                has_sensitive, detected_words = detect_sensitive_content(default_review)
                if has_sensitive:
                    mark_violation_user(conn, user_id, order_id, detected_words, default_review)

    logger.info(f"AI生成失败，使用默认{review_type}")
    return default_review

def check_and_create_table(conn):
    """检查表是否存在，如果不存在则创建，如果存在则添加缺失字段"""
    try:
        cursor = conn.cursor()

        # 首先检查表是否存在
        check_table_sql = """
        IF OBJECT_ID('oms_order_dtl_enhanced2', 'U') IS NOT NULL
        BEGIN
            SELECT 1
        END
        ELSE
        BEGIN
            SELECT 0
        END
        """
        cursor.execute(check_table_sql)
        table_exists = cursor.fetchone()[0]

        if table_exists == 0:
            # 表不存在，创建新表
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
                has_sensitive_content BIT DEFAULT 0,
                sensitive_words NVARCHAR(500),
                violation_handled BIT DEFAULT 0,
                ds DATE,
                created_time DATETIME DEFAULT GETDATE()
            )
            """
            cursor.execute(create_table_sql)
            logger.info("新表创建成功: oms_order_dtl_enhanced2")
        else:
            # 表已存在，检查并添加缺失字段
            logger.info("表已存在: oms_order_dtl_enhanced2，检查字段完整性...")

            # 检查字段是否存在，如果不存在则添加
            columns_to_check = [
                ('has_sensitive_content', 'BIT DEFAULT 0'),
                ('sensitive_words', 'NVARCHAR(500)'),
                ('violation_handled', 'BIT DEFAULT 0')
            ]

            for column_name, column_type in columns_to_check:
                check_column_sql = f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns 
                    WHERE object_id = OBJECT_ID('oms_order_dtl_enhanced2') 
                    AND name = '{column_name}'
                )
                BEGIN
                    ALTER TABLE oms_order_dtl_enhanced2 ADD {column_name} {column_type}
                    PRINT '添加字段: {column_name}'
                END
                """
                cursor.execute(check_column_sql)

            logger.info("表字段检查完成")

        conn.commit()

    except Exception as e:
        logger.error(f"检查/创建表失败: {e}")
        conn.rollback()
        raise

def insert_data_to_sqlserver(conn, df):
    """将数据插入到SQL Server，避免重复插入"""
    try:
        cursor = conn.cursor()

        # 插入SQL - 使用正确的字段名
        insert_sql = """
        INSERT INTO oms_order_dtl_enhanced2
        (order_id, user_id, product_id, product_name, brand, english_name, chinese_name, ai_review, has_sensitive_content, sensitive_words, ds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            try:
                # 直接插入，依赖数据库的唯一约束来避免重复
                cursor.execute(insert_sql, (
                    str(row['order_id']),
                    str(row['user_id']),
                    str(row['product_id']),
                    str(row['product_name'])[:500],  # 限制长度
                    str(row['brand'])[:100],
                    str(row['english_name'])[:200],
                    str(row['chinese_name'])[:300],
                    str(row['ai_review'])[:1000],
                    int(row['has_sensitive_content']),
                    str(row['sensitive_words'])[:500],
                    row['ds']
                ))
                inserted_count += 1
            except pymssql.IntegrityError:
                # 如果遇到重复键错误，跳过
                skipped_count += 1
                continue
            except Exception as e:
                logger.warning(f"插入数据时遇到错误，跳过该条记录: {e}")
                skipped_count += 1
                continue

        conn.commit()
        logger.info(f"成功插入 {inserted_count} 条新数据，跳过 {skipped_count} 条重复或错误数据")

    except Exception as e:
        logger.error(f"插入数据失败: {e}")
        conn.rollback()
        raise

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
        logger.info("SQL Server数据库连接成功!")

        # 显示敏感词分类器统计信息
        stats = classifier.get_statistics()
        logger.info(f"敏感词分类器统计: P0-{stats['p0_count']}个, P1-{stats['p1_count']}个, P2-{stats['p2_count']}个, 总计-{stats['total_count']}个")

        # 检查并创建表（如果不存在）- 修复版本
        check_and_create_table(conn)

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
        logger.info(f"获取到 {len(df)} 条原始数据")

        # 显示ds字段的分布情况
        if 'ds' in df.columns:
            ds_counts = df['ds'].value_counts().head(5)
            logger.info(f"DS字段分布（前5个）:")
            for ds, count in ds_counts.items():
                logger.info(f"  {ds}: {count}条")

        # 处理产品名称拆分
        logger.info("开始处理产品信息拆分...")
        results = []
        for product_name in df['product_name']:
            result = split_product_info(product_name)
            results.append(result)

        # 合并结果
        result_df = pd.DataFrame(results)
        final_df = pd.concat([df, result_df], axis=1)

        # 为每个产品生成AI评论
        logger.info("开始使用AI生成评论...")
        reviews = []
        sensitive_flags = []
        sensitive_words_list = []

        for i, row in final_df.iterrows():
            logger.info(f"\n正在处理第 {i + 1}/{len(final_df)} 条数据...")
            logger.info(f"  产品: {row['brand']} - {row['english_name']}")
            if 'ds' in row:
                logger.info(f"  日期: {row['ds']}")

            product_info = {
                'brand': row['brand'],
                'english_name': row['english_name'],
                'chinese_name': row['chinese_name']
            }

            # 使用AI生成评论
            review = generate_ai_review(product_info, row['user_id'], row['order_id'], conn)

            # 检测评论中的敏感内容
            has_sensitive, detected_words = detect_sensitive_content(review)

            reviews.append(review)
            sensitive_flags.append(1 if has_sensitive else 0)
            sensitive_words_list.append(','.join(detected_words) if detected_words else '')

            # 添加延迟避免API限制
            logger.info("等待3秒...")
            time.sleep(3)

        # 添加评论列和敏感词检测结果
        final_df['ai_review'] = reviews
        final_df['has_sensitive_content'] = sensitive_flags
        final_df['sensitive_words'] = sensitive_words_list

        # 显示处理统计
        sensitive_count = sum(sensitive_flags)
        logger.info(f"\n评论生成完成！敏感内容统计:")
        logger.info(f"- 总评论数: {len(reviews)}")
        logger.info(f"- 含敏感内容: {sensitive_count}")
        logger.info(f"- 敏感内容比例: {sensitive_count/len(reviews)*100:.1f}%")

        # 显示前5条结果
        logger.info("\n前5条处理结果:")
        logger.info("=" * 120)
        for i, row in final_df.head(5).iterrows():
            logger.info(f"产品 {i + 1}:")
            logger.info(f"  订单ID: {row['order_id']}")
            logger.info(f"  用户ID: {row['user_id']}")
            logger.info(f"  品牌: {row['brand']}")
            logger.info(f"  品类名称: {row['english_name']}")
            logger.info(f"  中文描述: {row['chinese_name']}")
            if 'ds' in row:
                logger.info(f"  日期: {row['ds']}")
            logger.info(f"  AI评论: {row['ai_review']}")
            logger.info(f"  含敏感内容: {'是' if row['has_sensitive_content'] else '否'}")
            if row['sensitive_words']:
                logger.info(f"  敏感词: {row['sensitive_words']}")
            logger.info("-" * 120)

        # 保存到CSV文件（备份）
        output_file = "oms_order_dtl_enhanced2_ai.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"\n数据已备份到CSV文件: {output_file}")

        # 插入到SQL Server新表（避免重复）
        logger.info("正在将数据插入到SQL Server...")
        insert_data_to_sqlserver(conn, final_df)

        # 处理违规用户
        logger.info("开始处理违规用户...")
        process_violation_users(conn)

        # 验证SQL Server插入的数据
        verify_sql = "SELECT COUNT(*) as total_count FROM oms_order_dtl_enhanced2"
        cursor = conn.cursor()
        cursor.execute(verify_sql)
        count = cursor.fetchone()[0]
        logger.info(f"SQL Server新表中现有数据量: {count} 条")

        # 显示敏感内容统计
        sensitive_stats_sql = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN has_sensitive_content = 1 THEN 1 ELSE 0 END) as sensitive_count
        FROM oms_order_dtl_enhanced2
        """
        cursor.execute(sensitive_stats_sql)
        stats = cursor.fetchone()
        logger.info(f"敏感内容统计 - 总数: {stats[0]}, 含敏感内容: {stats[1]}")

        conn.close()

        logger.info("\n处理完成！")
        logger.info(f"- 原始数据: {len(df)} 条")
        logger.info(f"- 存储位置: SQL Server表 [oms_order_dtl_enhanced2]")
        logger.info(f"- 新增字段: brand, english_name, chinese_name, ai_review, has_sensitive_content, sensitive_words")
        logger.info(f"- 备份文件: {output_file}")
        logger.info(f"- 违规用户处理: 已完成")

    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()