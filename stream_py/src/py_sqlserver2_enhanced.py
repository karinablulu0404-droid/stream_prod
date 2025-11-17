import logging
import os
import random
import re
import time
import warnings
from datetime import datetime
from typing import Dict, List, Set, Any

import pymssql
import requests
from dotenv import load_dotenv

# 导入jieba
try:
    import jieba
    import jieba.posseg as pseg
    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False

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
MODEL_NAME = "Qwen/Qwen2-7B-Instruct"

class IKAnalyzer:
    """IK分词器 - 基于jieba的细粒度分词"""

    def __init__(self):
        self.initialized = False
        if JIEBA_AVAILABLE:
            try:
                # 初始化jieba
                jieba.initialize()

                # 设置细粒度分词模式
                self._setup_fine_grained_mode()

                # 加载敏感词自定义词典
                self._load_sensitive_dict()

                self.initialized = True
                logger.info("🎯 IK分词器初始化成功 - 细粒度模式已启用")
            except Exception as e:
                logger.warning(f"IK分词器初始化失败: {e}")
                self.initialized = False
        else:
            logger.warning("❌ jieba不可用，IK分词器无法初始化")

    def _setup_fine_grained_mode(self):
        """设置细粒度分词模式"""
        # 强制切分敏感词组合
        fine_grained_words = [
            ('傻', '逼'), ('草', '泥', '马'), ('尼', '玛'), ('法', '克'),
            ('地', '域', '黑'), ('地', '图', '炮'), ('白', '皮', '猪'),
            ('死', '全', '家'), ('狗', '日', '的'), ('王', '八', '蛋')
        ]

        for word_parts in fine_grained_words:
            jieba.suggest_freq(word_parts, True)

    def _load_sensitive_dict(self):
        """加载敏感词自定义词典"""
        sensitive_words = [
            # P0级别敏感词
            '毒品', '枪支', '爆炸', '恐怖', '分裂', '邪教', '诈骗', '赌博', '色情',
            # P1级别敏感词
            '傻逼', '操你', '妈的', '尼玛', '草泥马', '法克', '废物',  '蠢货', '白痴',
            '地域黑', '地图炮', '尼哥', '黑鬼', '白皮猪', '我操', '真他妈',
            # P2级别敏感词
            '死全家', '去死', '王八蛋', '狗日的', '滚蛋', '特么的', '靠', '日', '草'
        ]

        for word in sensitive_words:
            jieba.add_word(word, freq=100000, tag='sensitive')

    def fine_grained_cut(self, text: str) -> List[str]:
        """细粒度分词 - 类似IK分词器的效果"""
        if not text or not isinstance(text, str):
            return []

        if not self.initialized:
            return self._simple_cut(text)

        try:
            # 第一步：精确模式分词
            words = list(jieba.cut(text, cut_all=False))
            result = []

            for word in words:
                # 对长词进行进一步细分
                if len(word) > 2:
                    sub_words = self._check_further_cut(word)
                    result.extend(sub_words)
                else:
                    result.append(word)

            # 第二步：使用搜索引擎模式进行补充切分
            search_words = list(jieba.cut_for_search(text))
            if len(search_words) > len(result):
                # 如果搜索引擎模式切分更细，使用其结果
                result = search_words

            logger.debug(f"🔍 IK分词结果: {words} -> {result}")
            return result

        except Exception as e:
            logger.warning(f"IK分词失败: {e}")
            return self._simple_cut(text)

    def _check_further_cut(self, word: str) -> List[str]:
        """检查长词是否需要进一步切分"""
        # 敏感词组件库
        sensitive_components = {
            '傻', '逼', '操', '尼', '玛', '草', '泥', '马', '法', '克',
            '垃', '圾', '废', '物', '蠢', '货', '白', '痴', '黑', '鬼',
            '狗', '日', '王', '八', '蛋', '滚', '蛋'
        }

        # 如果包含敏感词成分，尝试进一步切分
        if any(comp in word for comp in sensitive_components):
            try:
                # 使用搜索引擎模式进行更细粒度的切分
                fine_words = list(jieba.cut_for_search(word))
                if len(fine_words) > 1:
                    return fine_words
            except:
                pass

        return [word]

    def _simple_cut(self, text: str) -> List[str]:
        """简单分词回退"""
        delimiters = '，。！？；：""''（）【】《》〈〉、\n\t ,.!?;:"''()[]{}'
        pattern = f'([{re.escape(delimiters)}])'
        segments = re.split(pattern, text)
        return [seg.strip() for seg in segments if seg.strip()]

    def analyze_text_components(self, text: str) -> Dict[str, Any]:
        """分析文本成分"""
        words = self.fine_grained_cut(text)

        return {
            'words': words,
            'word_count': len(words),
            'char_count': len(text),
            'unique_words': list(set(words)),
            'word_length_distribution': [len(word) for word in words],
            'analyzer_type': 'ik_analyzer'
        }

# 初始化IK分词器
ik_analyzer = IKAnalyzer()

class EnhancedSensitiveWordClassifier:
    """增强版敏感词分类器 - 集成IK分词器"""

    def __init__(self, use_ik_analyzer: bool = True):
        self.p0_words: Set[str] = set()
        self.p1_words: Set[str] = set()
        self.p2_words: Set[str] = set()
        self._word_categories: Dict[str, str] = {}
        self.use_ik_analyzer = use_ik_analyzer
        self.ik_analyzer = ik_analyzer

        # 基础敏感词库
        self.base_sensitive_words = {
            "p0": {"毒品", "枪支", "爆炸", "恐怖", "分裂", "邪教", "诈骗", "赌博", "色情"},
            "p1": {"傻逼", "操你", "妈的", "尼玛", "草泥马", "法克", "废物", "蠢货", "白痴", "我操", "真他妈", "狗日的", "烂货"},  # 添加烂货
            "p2": {"地域黑", "地图炮", "死全家", "去死", "白皮猪", "黑鬼", "滚蛋", "王八蛋", "特么的", "靠", "日", "草"}
        }

        # 构建敏感词变体库
        self._build_sensitive_variants()

    def _build_sensitive_variants(self):
        """构建敏感词变体库"""
        self.sensitive_variants = {
            # 拼音变体
            '傻逼': ['sb', 'SB', 'Sb', 'sB', '傻B', '傻b'],
            '妈的': ['md', 'MD', 'Md', '马德', '麻的'],
            '尼玛': ['nm', 'NM', 'Nm', '你妈', '尼马'],
            '草泥马': ['cnm', 'CNM', 'Cnm', '草尼马'],
            '法克': ['fk', 'FK', 'Fk'],

            # 谐音变体
            '傻逼': ['沙比', '煞笔', '傻比', '啥比'],
            '废物': ['费物', '废武'],

            # 拆字变体
            '傻逼': ['傻 逼', '傻-逼', '傻_逼', '傻·逼'],
            '草泥马': ['草 泥 马', '草-泥-马', '草_泥_马'],
            '尼玛': ['尼 玛', '尼-玛', '尼_玛'],
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
        p1_keywords = ['傻逼', '操你', '妈的', '地域黑', '地图炮', '尼哥', '尼玛', '草泥马', '法克', '黑鬼', '白皮猪', '我操', '真他妈', '狗日的']
        p2_keywords = ['死全家', '去死', '滚蛋', '王八蛋', '特么的', '靠', '日', '草']

        if any(keyword in word for keyword in p0_keywords):
            return "p0"
        elif any(keyword in word for keyword in p1_keywords):
            return "p1"
        elif any(keyword in word for keyword in p2_keywords):
            return "p2"
        else:
            return "p2"

    def check_sensitive_enhanced(self, text: str) -> Dict[str, List[str]]:
        """修正版敏感词检测 - 只检测输入文本"""
        if not text or not isinstance(text, str):
            return {"p0": [], "p1": [], "p2": []}

        result = {"p0": [], "p1": [], "p2": []}

        if self.use_ik_analyzer and self.ik_analyzer.initialized:
            # 对输入文本进行分词
            words = self.ik_analyzer.fine_grained_cut(text)
            found_words = set()

            # 检查每个分词是否在敏感词库中
            for word in words:
                # 检查原词
                if word in self._word_categories:
                    category = self._word_categories[word]
                    found_words.add((word, category))

                # 检查小写版本
                word_lower = word.lower()
                if word_lower in self._word_categories:
                    category = self._word_categories[word_lower]
                    found_words.add((word_lower, category))

                # 检查变体 - 传递text参数
                self._check_variants_for_word(word, found_words, text)  # 修复：添加text参数

            # 整理结果
            for word, category in found_words:
                result[category].append(word)
        else:
            # 回退到普通匹配
            text_lower = text.lower()
            for word, category in self._word_categories.items():
                if word in text or word.lower() in text_lower:
                    result[category].append(word)

        # 去重
        for category in result:
            result[category] = list(set(result[category]))

        return result

    def _check_variants_for_word(self, word: str, found_words: set, text: str):
        """检查单个词的敏感词变体"""
        for sensitive_word, variants in self.sensitive_variants.items():
            # 检查当前词是否是敏感词的变体
            if word in variants:
                category = self._word_categories.get(sensitive_word, "p2")
                found_words.add((sensitive_word, category))
            # 检查敏感词的其他变体是否在文本中
            for variant in variants:
                if variant in text and variant != word:
                    category = self._word_categories.get(sensitive_word, "p2")
                    found_words.add((sensitive_word, category))


    def get_detailed_analysis(self, text: str) -> Dict[str, Any]:
        """获取详细的敏感词分析报告"""
        sensitive_result = self.check_sensitive_enhanced(text)

        if self.use_ik_analyzer and self.ik_analyzer.initialized:
            ik_analysis = self.ik_analyzer.analyze_text_components(text)
        else:
            ik_analysis = {"words": [], "word_count": 0, "char_count": len(text)}

        total_sensitive = sum(len(words) for words in sensitive_result.values())

        return {
            "sensitive_words": sensitive_result,
            "total_sensitive_count": total_sensitive,
            "has_sensitive": total_sensitive > 0,
            "text_analysis": ik_analysis,
            "detection_method": "IK分词器" if (self.use_ik_analyzer and self.ik_analyzer.initialized) else "普通匹配"
        }

    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            "p0_count": len(self.p0_words),
            "p1_count": len(self.p1_words),
            "p2_count": len(self.p2_words),
            "total_count": len(self._word_categories)
        }

# 初始化增强版敏感词分类器
enhanced_classifier = EnhancedSensitiveWordClassifier(use_ik_analyzer=True)
enhanced_classifier.load_sensitive_words()

def detect_sensitive_content_enhanced(text):
    """修正版敏感内容检测 - 只检测输入的文本"""
    if not isinstance(text, str):
        return False, [], {}

    # 使用IK分词器对输入的评论进行分词
    words = ik_analyzer.fine_grained_cut(text)

    # 检测分词结果中的敏感词
    sensitive_result = enhanced_classifier.check_sensitive_enhanced(text)  # 这里会调用修复后的方法

    # 合并所有级别的敏感词
    all_detected_words = []
    for category in ["p0", "p1", "p2"]:
        all_detected_words.extend(sensitive_result[category])

    has_sensitive = len(all_detected_words) > 0

    analysis_result = {
        "sensitive_words": sensitive_result,
        "total_sensitive_count": len(all_detected_words),
        "has_sensitive": has_sensitive,
        "text_analysis": {
            "words": words,
            "word_count": len(words),
            "char_count": len(text)
        },
        "detection_method": "IK分词器"
    }

    return has_sensitive, all_detected_words, analysis_result

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
            category = enhanced_classifier._word_categories.get(word, "p2")
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
    """处理违规用户，并更新oms_order_dtl_enhanced2表中的violation_handled字段"""
    try:
        cursor = conn.cursor()

        # 获取未处理的高风险违规用户
        query_sql = """
        SELECT user_id, order_id, violation_level, sensitive_words, p0_count, p1_count, p2_count, created_time
        FROM user_violation_records 
        WHERE handled = 0 AND violation_level IN ('CRITICAL', 'HIGH', 'MEDIUM')
        ORDER BY created_time DESC
        """

        cursor.execute(query_sql)
        violations = cursor.fetchall()

        for violation in violations:
            user_id, order_id, level, words, p0_count, p1_count, p2_count, create_time = violation
            logger.info(f"处理违规用户: {user_id}, 订单: {order_id}, 级别: {level}, P0: {p0_count}, P1: {p1_count}, P2: {p2_count}, 敏感词: {words}")

            # 更新oms_order_dtl_enhanced2表中的violation_handled字段
            update_oms_sql = """
            UPDATE oms_order_dtl_enhanced2 
            SET violation_handled = 1 
            WHERE user_id = %s AND order_id = %s AND has_sensitive_content = 1
            """
            cursor.execute(update_oms_sql, (user_id, order_id))

            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.info(f"  已更新 {updated_count} 条记录在oms_order_dtl_enhanced2表中的violation_handled字段")

            # 标记user_violation_records为已处理
            update_violation_sql = "UPDATE user_violation_records SET handled = 1 WHERE user_id = %s AND order_id = %s AND handled = 0"
            cursor.execute(update_violation_sql, (user_id, order_id))

        conn.commit()
        logger.info(f"已处理 {len(violations)} 个违规用户，并更新了oms_order_dtl_enhanced2表中的violation_handled字段")

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
    使用硅基流动API生成商品评论，差评有50%概率包含敏感词
    返回评论内容和敏感词检测结果
    """
    brand = product_info.get('brand', '')
    english_name = product_info.get('english_name', '')
    chinese_name = product_info.get('chinese_name', '')

    # 随机决定评论类型：50%好评，30%中评，20%差评
    random_num = random.random()

    if random_num < 0.50:
        # 好评 50%
        review_type = "好评"
        sentiment_instruction = "表达满意和推荐，语气积极正面"
        include_sensitive = False
    elif random_num < 0.80:
        # 中评 30%
        review_type = "中评"
        sentiment_instruction = "表达中立客观，既有优点也有缺点，语气平和"
        include_sensitive = False
    else:
        # 差评 20%
        review_type = "差评"
        # 差评有35%概率包含敏感词
        include_sensitive = random.random() < 0.35
        if include_sensitive:
            sentiment_instruction = "表达极度愤怒和不满，使用强烈的情绪化语言，可以包含一些网络用语和情绪发泄"
            logger.info(f"🎯 本次差评将尝试生成包含敏感词的内容")
        else:
            sentiment_instruction = "表达不满和批评，语气强烈"

    # 构建提示词 - 对于包含敏感词的差评，使用更直接的提示
    if include_sensitive and review_type == "差评":
        prompt = f"""
        请为以下商品生成一条极度愤怒的差评，表达强烈不满：
        品牌：{brand}
        产品名称：{english_name}
        产品描述：{chinese_name}

        要求：
        1. 评论长度在15-40字之间
        2. 语气极其愤怒，像真实用户在发泄情绪
        3. 使用强烈的情绪化表达，可以适当使用网络用语
        4. 表达产品质量极差、服务糟糕、完全不值得购买
        5. 直接返回评论内容，不要添加其他说明
        6. 不用思考，直接表达愤怒
        """
    else:
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
        "temperature": 0.9 if include_sensitive else 0.8,  # 包含敏感词时增加随机性
        "top_p": 0.9 if include_sensitive else 0.8,
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

                    # 使用修正版敏感词检测方法（IK分词器）- 只检测生成的评论
                    has_sensitive, detected_words, analysis_detail = detect_sensitive_content_enhanced(review)

                    logger.info(f"🔍 IK分词器检测报告:")
                    logger.info(f"  - 原始评论: {review}")
                    logger.info(f"  - 分词数量: {analysis_detail['text_analysis']['word_count']}")
                    logger.info(f"  - 检测方法: {analysis_detail['detection_method']}")
                    logger.info(f"  - 发现敏感词: {detected_words}")
                    logger.info(f"  - 详细分析: {analysis_detail['sensitive_words']}")

                    if has_sensitive:
                        # 标记违规用户
                        mark_violation_user(conn, user_id, order_id, detected_words, review)
                        # 替换敏感词
                        for word in detected_words:
                            review = review.replace(word, "***")
                        logger.warning(f"🚨 检测到敏感词并已处理: {detected_words}")

                        # 返回包含敏感词检测结果的数据
                        return review, has_sensitive, detected_words
                    else:
                        if include_sensitive and review_type == "差评":
                            logger.info("  📝 本次差评尝试生成敏感词但未成功，AI可能进行了自我审查")

                        # 返回无敏感词的数据
                        return review, False, []

                    logger.info(f"  {review_type}生成成功: {review}")

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
    if random_num < 0.50:
        default_review = f"{brand}的{english_name}很不错，使用体验很好，值得推荐。"
        return default_review, False, []
    elif random_num < 0.80:
        default_review = f"{brand}的{english_name}整体还可以，有些地方不错，但也有一些小问题。"
        return default_review, False, []
    else:
        # 对于差评，有50%概率在默认评论中加入敏感词
        if random.random() < 0.5:
            sensitive_words = [ "废物", "坑爹", "骗钱"]
            sensitive_word = random.choice(sensitive_words)
            default_review = f"{brand}的{english_name}真是{sensitive_word}！质量差到爆，完全浪费钱！"
            # 检测默认评论的敏感词
            has_sensitive, detected_words, _ = detect_sensitive_content_enhanced(default_review)
            if has_sensitive:
                mark_violation_user(conn, user_id, order_id, detected_words, default_review)
                return default_review, True, detected_words
            else:
                return default_review, False, []
        else:
            default_review = f"{brand}的{english_name}质量一般，有些失望，不太推荐。"
            return default_review, False, []

    logger.info(f"AI生成失败，使用默认{review_type}")

def check_and_create_table(conn):
    """检查表是否存在，如果不存在则创建，如果存在则添加缺失字段（包含金额字段）"""
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
            # 表不存在，创建新表（包含金额字段）
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
                sale_amount DECIMAL(18,2) DEFAULT 0.00, 
                total_amount DECIMAL(18,2) DEFAULT 0.00,-- 新增付款金额字段
                ds DATE,
                created_time DATETIME DEFAULT GETDATE()
            )
            """
            cursor.execute(create_table_sql)
            logger.info("新表创建成功: oms_order_dtl_enhanced2（包含金额字段）")
        else:
            # 表已存在，检查并添加缺失字段
            logger.info("表已存在: oms_order_dtl_enhanced2，检查字段完整性...")

            # 检查字段是否存在，如果不存在则添加
            columns_to_check = [
                ('has_sensitive_content', 'BIT DEFAULT 0'),
                ('sensitive_words', 'NVARCHAR(500)'),
                ('violation_handled', 'BIT DEFAULT 0'),
                ('sale_amount', 'DECIMAL(18,2) DEFAULT 0.00'),  # 新增金额字段检查
                ('total_amount', 'DECIMAL(18,2) DEFAULT 0.00')
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

def insert_single_record_to_sqlserver(conn, record):
    """将单条记录插入到SQL Server，避免重复插入（包含金额字段）"""
    try:
        cursor = conn.cursor()

        # 插入SQL - 包含violation_handled和金额字段
        insert_sql = """
        INSERT INTO oms_order_dtl_enhanced2
        (order_id, user_id, product_id, product_name, brand, english_name, chinese_name, ai_review, has_sensitive_content, sensitive_words, violation_handled, sale_amount, total_amount, ds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            # 检查是否有敏感词并正确设置字段值
            has_sensitive_content = 1 if record['has_sensitive_content'] else 0
            sensitive_words = str(record['sensitive_words']) if record['sensitive_words'] else ''

            # 如果有敏感词，自动标记为已处理
            violation_handled = 1 if has_sensitive_content else 0  # 自动设置为1

            # 金额字段
            sale_amount = record.get('sale_amount', 0.00)
            total_amount = record.get('total_amount', 0.00)

            # 直接插入，依赖数据库的唯一约束来避免重复
            cursor.execute(insert_sql, (
                str(record['order_id']),
                str(record['user_id']),
                str(record['product_id']),
                str(record['product_name'])[:500],
                str(record['brand'])[:100],
                str(record['english_name'])[:200],
                str(record['chinese_name'])[:300],
                str(record['ai_review'])[:1000],
                has_sensitive_content,
                sensitive_words[:500],
                violation_handled,  # 使用自动设置的值
                sale_amount,
                total_amount,
                record['ds']
            ))

            conn.commit()
            logger.info(f"✅ 成功插入记录 - 订单: {record['order_id']}, 用户: {record['user_id']}, 敏感词: {sensitive_words}, 违规处理: {violation_handled}")

            # 如果有敏感词，立即处理违规用户
            if has_sensitive_content and sensitive_words:
                sensitive_word_list = sensitive_words.split(',') if ',' in sensitive_words else [sensitive_words]
                mark_violation_user(conn, record['user_id'], record['order_id'], sensitive_word_list, record['ai_review'])
                logger.info(f"🚨 自动标记违规用户: {record['user_id']}, 敏感词: {sensitive_words}")

            return True

        except pymssql.IntegrityError:
            # 如果遇到重复键错误，跳过
            logger.info(f"⏭️ 跳过重复记录 - 订单: {record['order_id']}")
            conn.rollback()
            return False
        except Exception as e:
            logger.warning(f"插入数据时遇到错误，跳过该条记录: {e}")
            conn.rollback()
            return False

    except Exception as e:
        logger.error(f"插入数据失败: {e}")
        conn.rollback()
        return False

def process_single_order_record(conn, order_record):
    """处理单条订单记录（包含金额字段）"""
    try:
        logger.info(f"\n正在处理订单记录...")
        logger.info(f"  订单ID: {order_record['order_id']}")
        logger.info(f"  用户ID: {order_record['user_id']}")
        logger.info(f"  产品名称: {order_record['product_name']}")
        logger.info(f"  销售金额: {order_record.get('sale_amount', 0.00)}")
        logger.info(f"  总金额: {order_record.get('total_amount', 0.00)}")
        if 'ds' in order_record:
            logger.info(f"  日期: {order_record['ds']}")

        # 处理产品名称拆分
        product_info = split_product_info(order_record['product_name'])

        # 使用AI生成评论，并获取敏感词检测结果
        review, has_sensitive, detected_words = generate_ai_review(
            product_info,
            order_record['user_id'],
            order_record['order_id'],
            conn
        )

        # 构建结果记录
        result_record = {
            'order_id': order_record['order_id'],
            'user_id': order_record['user_id'],
            'product_id': order_record['product_id'],
            'product_name': order_record['product_name'],
            'brand': product_info['brand'],
            'english_name': product_info['english_name'],
            'chinese_name': product_info['chinese_name'],
            'ai_review': review,
            'has_sensitive_content': has_sensitive,
            'sensitive_words': ','.join(detected_words) if detected_words else '',
            'sale_amount': order_record.get('sale_amount', 0.00),  # 添加销售金额
            'total_amount': order_record.get('total_amount', 0.00),  # 添加总金额
            'ds': order_record.get('ds', None)
        }

        # 插入到数据库
        success = insert_single_record_to_sqlserver(conn, result_record)

        if success:
            logger.info(f"✅ 订单 {order_record['order_id']} 处理完成")
            if has_sensitive:
                logger.warning(f"🚨 该订单包含敏感词: {detected_words}")
        else:
            logger.error(f"❌ 订单 {order_record['order_id']} 处理失败")

        return success

    except Exception as e:
        logger.error(f"处理订单记录失败: {e}")
        return False

def get_table_structure(conn, table_name):
    """获取表结构信息"""
    try:
        cursor = conn.cursor()
        query_sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query_sql)
        columns = cursor.fetchall()

        logger.info(f"表 {table_name} 结构:")
        for column in columns:
            logger.info(f"  - {column[0]}: {column[1]}")

        return [col[0] for col in columns]
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        return []

def get_new_orders(conn, last_processed_time=None, batch_size=10):
    """获取新的订单记录（基于时间戳的实时处理模式）- 包含金额字段"""
    try:
        cursor = conn.cursor()

        # 如果没有最后处理时间，使用当前时间减去1小时
        if last_processed_time is None:
            last_processed_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 首先检查oms_order_dtl表的结构
        columns = get_table_structure(conn, 'oms_order_dtl')

        # 构建查询，基于可用的时间字段
        time_field = None
        if 'created_time' in columns:
            time_field = 'created_time'
        elif 'update_time' in columns:
            time_field = 'update_time'
        elif 'ds' in columns:
            time_field = 'ds'

        # 检查源表是否有金额字段
        has_sale_amount = 'sale_amount' in columns
        has_total_amount = 'total_amount' in columns

        # 选择金额字段，如果不存在则使用NULL
        sale_amount_field = 'sale_amount' if has_sale_amount else 'NULL as sale_amount'
        total_amount_field = 'total_amount' if has_total_amount else 'NULL as total_amount'

        if time_field:
            # 使用时间字段进行查询
            query_sql = f"""
            SELECT TOP {batch_size} 
                order_id,
                user_id,
                product_id,
                product_name,
                {sale_amount_field},
                {total_amount_field},
                ds
            FROM oms_order_dtl 
            WHERE {time_field} > '{last_processed_time}'
            ORDER BY {time_field} ASC
            """
        else:
            # 如果没有时间字段，使用固定查询
            logger.warning("未找到时间字段，使用固定查询")
            query_sql = f"""
            SELECT TOP {batch_size} 
                order_id,
                user_id,
                product_id,
                product_name,
                {sale_amount_field},
                {total_amount_field},
                ds
            FROM oms_order_dtl 
            ORDER BY order_id
            """

        cursor.execute(query_sql)
        orders = cursor.fetchall()

        # 转换为字典列表
        order_list = []
        for order in orders:
            order_dict = {
                'order_id': order[0],
                'user_id': order[1],
                'product_id': order[2],
                'product_name': order[3],
                'sale_amount': float(order[4]) if order[4] is not None else 0.00,
                'total_amount': float(order[5]) if order[5] is not None else 0.00,
                'ds': order[6] if len(order) > 6 else None
            }
            order_list.append(order_dict)

        logger.info(f"获取到 {len(order_list)} 条新订单")
        # 如果有金额数据，记录日志
        if has_sale_amount or has_total_amount:
            logger.info(f"金额字段状态: sale_amount={has_sale_amount}, total_amount={has_total_amount}")
        return order_list

    except Exception as e:
        logger.error(f"获取新订单失败: {e}")
        return []

def get_last_processed_time(conn):
    """获取最后处理的时间"""
    try:
        cursor = conn.cursor()

        # 从目标表获取最后处理的时间
        query_sql = """
        SELECT MAX(created_time) 
        FROM oms_order_dtl_enhanced2
        """

        cursor.execute(query_sql)
        result = cursor.fetchone()

        if result and result[0]:
            return result[0].strftime('%Y-%m-%d %H:%M:%S')
        else:
            # 如果没有记录，返回1小时前的时间
            one_hour_ago = datetime.now().replace(hour=datetime.now().hour-1)
            return one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')

    except Exception as e:
        logger.error(f"获取最后处理时间失败: {e}")
        # 返回1小时前的时间作为默认值
        one_hour_ago = datetime.now().replace(hour=datetime.now().hour-1)
        return one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')

def real_time_processing_loop(conn, check_interval=30, batch_size=5):
    """实时处理循环"""
    logger.info("🚀 启动实时处理模式...")

    # 获取最后处理的时间
    last_processed_time = get_last_processed_time(conn)
    logger.info(f"📊 最后处理时间: {last_processed_time}")

    processed_count = 0
    error_count = 0

    while True:
        try:
            # 获取新订单
            new_orders = get_new_orders(conn, last_processed_time, batch_size)

            if new_orders:
                logger.info(f"📥 获取到 {len(new_orders)} 条新订单")

                for order in new_orders:
                    # 处理单条订单
                    success = process_single_order_record(conn, order)

                    if success:
                        processed_count += 1
                    else:
                        error_count += 1
                        # 无论成功失败都更新时间戳，避免重复处理
                    last_processed_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # 添加延迟避免API限制
                    logger.info("等待3秒...")
                    time.sleep(3)

                logger.info(f"📊 处理统计 - 成功: {processed_count}, 失败: {error_count}")

                # 处理违规用户
                logger.info("开始处理违规用户...")
                process_violation_users(conn)

            else:
                logger.info(f"⏳ 没有新订单，等待 {check_interval} 秒后重试...")

            # 等待指定间隔
            time.sleep(check_interval)

        except KeyboardInterrupt:
            logger.info("🛑 用户中断处理")
            break
        except Exception as e:
            logger.error(f"实时处理循环异常: {e}")
            error_count += 1
            time.sleep(check_interval)  # 出错时也等待

def main():
    """主函数 - 实时处理模式"""
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

        # 显示分类器信息
        stats = enhanced_classifier.get_statistics()
        logger.info(f"🎯 增强版敏感词分类器统计:")
        logger.info(f"  - P0严重违规: {stats['p0_count']}个")
        logger.info(f"  - P1脏话歧视: {stats['p1_count']}个")
        logger.info(f"  - P2一般敏感: {stats['p2_count']}个")
        logger.info(f"  - 总计: {stats['total_count']}个")
        logger.info(f"  - 检测方法: {'IK分词器' if enhanced_classifier.use_ik_analyzer else '普通匹配'}")
        logger.info(f"  - IK分词器状态: {'已启用' if ik_analyzer.initialized else '未启用'}")
        logger.info(f"  - jieba状态: {'✅ 可用' if JIEBA_AVAILABLE else '❌ 不可用'}")

        # 检查并创建表（如果不存在）- 修复版本
        check_and_create_table(conn)

        # 启动实时处理循环
        real_time_processing_loop(conn, check_interval=30, batch_size=5)

        conn.close()
        logger.info("\n🎉 实时处理结束！")

    except Exception as e:
        logger.error(f"实时处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()