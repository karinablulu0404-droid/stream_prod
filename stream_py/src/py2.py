# py_sqlserver2_enhanced.py
import logging
import os
import random
import re
import time
import warnings
from typing import Dict, List, Set, Any

import pymssql
import requests
from dotenv import load_dotenv

# 导入jieba
try:
    import jieba
    import jieba.posseg as pseg
    JIEBA_AVAILABLE = True
    logger_extra = "✅ jieba可用"
except ImportError:
    JIEBA_AVAILABLE = False
    logger_extra = "❌ jieba不可用"

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
            '傻逼', '操你', '妈的', '尼玛', '草泥马', '法克', '废物', '垃圾', '蠢货', '白痴',
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

            logger.debug(f"🔍 IK分词结果: {result}")
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
            "p1": {"傻逼", "操你", "妈的", "尼玛", "草泥马", "法克", "废物", "垃圾", "蠢货", "白痴"},
            "p2": {"地域黑", "地图炮", "死全家", "去死", "白皮猪", "黑鬼"}
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
            '垃圾': ['拉基', '辣鸡', '垃鸡'],

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
        p1_keywords = ['傻逼', '操你', '妈的', '地域黑', '地图炮', '尼哥', '尼玛', '草泥马', '法克', '黑鬼', '白皮猪']

        if any(keyword in word for keyword in p0_keywords):
            return "p0"
        elif any(keyword in word for keyword in p1_keywords):
            return "p1"
        elif len(word) <= 3:
            return "p1"
        else:
            return "p2"

    def check_sensitive_enhanced(self, text: str) -> Dict[str, List[str]]:
        """增强版敏感词检测 - 使用IK分词器"""
        if not text or not isinstance(text, str):
            return {"p0": [], "p1": [], "p2": []}

        result = {"p0": [], "p1": [], "p2": []}
        text_lower = text.lower()

        if self.use_ik_analyzer and self.ik_analyzer.initialized:
            # 使用IK分词器进行细粒度分词
            words = self.ik_analyzer.fine_grained_cut(text)
            found_words = set()

            logger.info(f"🔍 IK分词结果: {words}")

            # 在分词结果中检测敏感词
            for word in words:
                # 检查原词
                self._check_word_sensitive(word, found_words)

                # 检查小写版本
                self._check_word_sensitive(word.lower(), found_words)

                # 检查变体
                self._check_variants(word, found_words)

            # 直接匹配作为补充（处理IK可能漏掉的情况）
            for sensitive_word, category in self._word_categories.items():
                if (sensitive_word in text or
                        sensitive_word.lower() in text_lower):
                    found_words.add((sensitive_word, category))

            # 整理结果
            for word, category in found_words:
                result[category].append(word)
        else:
            # 回退到普通匹配
            for word, category in self._word_categories.items():
                if word in text or word.lower() in text_lower:
                    result[category].append(word)

        # 去重
        for category in result:
            result[category] = list(set(result[category]))

        return result

    def _check_word_sensitive(self, word: str, found_words: set):
        """检查单个词是否敏感"""
        for sensitive_word, category in self._word_categories.items():
            if (sensitive_word == word or
                    sensitive_word in word or
                    word in sensitive_word):
                found_words.add((sensitive_word, category))

    def _check_variants(self, word: str, found_words: set):
        """检查敏感词变体"""
        for sensitive_word, variants in self.sensitive_variants.items():
            if word in variants:
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

def get_random_element(lst):
    """从列表中随机返回一个元素"""
    if not lst:
        return None
    return random.choice(lst)

def detect_sensitive_content_enhanced(text):
    """增强版敏感内容检测 - 使用IK分词器"""
    if not isinstance(text, str):
        return False, [], {}

    analysis_result = enhanced_classifier.get_detailed_analysis(text)

    # 合并所有级别的敏感词
    all_detected_words = []
    for category in ["p0", "p1", "p2"]:
        all_detected_words.extend(analysis_result["sensitive_words"][category])

    has_sensitive = len(all_detected_words) > 0

    return has_sensitive, all_detected_words, analysis_result

# 原有的其他函数保持不变（mark_violation_user, process_violation_users, fix_encoding, split_product_info等）
# 这里只展示修改后的generate_ai_review函数

def generate_ai_review(product_info, user_id, order_id, conn, max_retries=3):
    """
    使用硅基流动API生成商品评论，加入增强版语言和脏话过滤
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

    # 脏话后缀列表
    sensitive_suffixes = [
        " 妈的", " 操", " 傻逼", " 垃圾", " 特么的", " 草", " 日", " 靠",
        " 尼玛", " 我操", " 真他妈", " 狗日的", " 滚蛋", " 去死", " 王八蛋"
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

                    # 使用增强版敏感词检测方法（IK分词器）
                    has_sensitive, detected_words, analysis_detail = detect_sensitive_content_enhanced(review)

                    logger.info(f"🔍 IK分词器检测报告:")
                    logger.info(f"  - 分词数量: {analysis_detail['text_analysis']['word_count']}")
                    logger.info(f"  - 检测方法: {analysis_detail['detection_method']}")

                    if has_sensitive:
                        # 标记违规用户
                        mark_violation_user(conn, user_id, order_id, detected_words, review)
                        # 替换敏感词
                        for word in detected_words:
                            review = review.replace(word, "***")
                        logger.warning(f"🚨 检测到敏感词并已处理: {detected_words}")
                        logger.info(f"  - 详细分析: {analysis_detail['sensitive_words']}")

                    # 为差评添加敏感词后缀
                    if review_type == "差评" and random.random() < 0.3:
                        suffix = get_random_element(sensitive_suffixes)
                        if suffix:
                            review += suffix
                            # 检测添加的后缀是否包含敏感词
                            has_sensitive_suffix, suffix_words, suffix_analysis = detect_sensitive_content_enhanced(suffix)
                            if has_sensitive_suffix:
                                mark_violation_user(conn, user_id, order_id, suffix_words, review)
                                logger.warning(f"🚨 后缀包含敏感词，用户已被标记: {suffix_words}")
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
        # 为差评默认评论添加敏感词后缀
        if random.random() < 0.3:
            suffix = get_random_element(sensitive_suffixes)
            if suffix:
                default_review += suffix
                # 检测默认评论的后缀
                has_sensitive, detected_words, _ = detect_sensitive_content_enhanced(default_review)
                if has_sensitive:
                    mark_violation_user(conn, user_id, order_id, detected_words, default_review)

    logger.info(f"AI生成失败，使用默认{review_type}")
    return default_review

# 其他原有函数保持不变（mark_violation_user, process_violation_users, fix_encoding, split_product_info, check_and_create_table, insert_data_to_sqlserver, main）

# 在main函数开头添加分类器信息显示
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

        # 显示分类器信息
        stats = enhanced_classifier.get_statistics()
        logger.info(f"🎯 增强版敏感词分类器统计:")
        logger.info(f"  - P0严重违规: {stats['p0_count']}个")
        logger.info(f"  - P1脏话歧视: {stats['p1_count']}个")
        logger.info(f"  - P2一般敏感: {stats['p2_count']}个")
        logger.info(f"  - 总计: {stats['total_count']}个")
        logger.info(f"  - 检测方法: {'IK分词器' if enhanced_classifier.use_ik_analyzer else '普通匹配'}")
        logger.info(f"  - IK分词器状态: {'已启用' if ik_analyzer.initialized else '未启用'}")
        logger.info(f"  - jieba状态: {logger_extra}")

        # 其余原有代码保持不变...
        # [原有的main函数代码]

    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()