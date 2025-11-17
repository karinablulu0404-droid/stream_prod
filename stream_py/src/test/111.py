import logging
import os
import re
# 过滤警告
import warnings
from typing import Dict, List, Set

from dotenv import load_dotenv

warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class SensitiveWordClassifier:
    """敏感词分类器 - 最终优化版本"""

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

# 使用示例
if __name__ == "__main__":
    classifier = SensitiveWordClassifier(use_enhanced_matching=True)
    classifier.load_sensitive_words()

    # 测试
    test_texts = [
        "这是一段正常的测试文本",
        "这条裤子尺码偏大，面料粗糙不透气，穿上后闷热不舒服，完全不符合宣传的修身和舒适体验，非常失望。 操",
        "这件lululemon Ribbed Nulu™运动内衣尺码偏大，面料闷热不透气，运动时容易滑动，完全不符合预期，非常失望。 傻逼",
        "这款lululemon男士长裤剪裁修身，面料舒适透气，穿上很轻盈，非常适合日常运动和休闲穿搭，强烈推荐",
        "这件lululemon的Ribbed Nulu™运动内衣质量太差了，穿了一次就起球，面料也不舒服，完全不值这个价格！ 真他妈",
        " 这款帽子质量一般，洗后容易变形，颜色也不如描述那么好看，戴着不舒服，完全不符合预期，不推荐购买。 滚蛋",
    ]

    for text in test_texts:
        result = classifier.check_sensitive(text)
        print(f"文本: {text}")
        print(f"检测结果: {result}\n")

    stats = classifier.get_statistics()
    print(f"统计信息: {stats}")