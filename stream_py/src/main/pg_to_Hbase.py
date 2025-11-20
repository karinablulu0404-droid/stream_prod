# pg_to_hbase_importer.py
import logging
import os
from datetime import datetime

import happybase
import psycopg2
from dotenv import load_dotenv

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class PGToHBaseImporter:
    def __init__(self):
        # PostgreSQL配置
        self.pg_host = os.getenv('PG_HOST', '127.0.0.1')
        self.pg_port = os.getenv('PG_PORT', '5432')
        self.pg_db = os.getenv('PG_DB')
        self.pg_user = os.getenv('PG_USER')
        self.pg_password = os.getenv('PG_PASSWORD')

        # HBase配置 - 使用WSL的IP地址
        self.hbase_host = '172.17.42.124'  # WSL IP地址
        self.hbase_port = 9090  # Thrift端口
        self.hbase_table_name = 'user_info_base'

        self.connection = None
        self.table = None

    def connect_postgresql(self):
        """连接PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_password
            )
            logger.info("✅ PostgreSQL连接成功!")
            return conn
        except Exception as e:
            logger.error(f"❌ PostgreSQL连接失败: {e}")
            return None

    def connect_hbase(self):
        """连接HBase"""
        try:
            self.connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                timeout=30000  # 30秒超时
            )
            logger.info(f"✅ HBase连接成功: {self.hbase_host}:{self.hbase_port}")
            return True
        except Exception as e:
            logger.error(f"❌ HBase连接失败: {e}")
            return False

    def setup_hbase_table(self):
        """设置HBase表"""
        try:
            tables = self.connection.tables()

            if self.hbase_table_name.encode() in tables:
                logger.info(f"📊 HBase表已存在: {self.hbase_table_name}")
                # 禁用并删除现有表（如果需要重新导入）
                self.connection.disable_table(self.hbase_table_name)
                self.connection.delete_table(self.hbase_table_name)
                logger.info("已删除现有表")

            # 创建新表
            families = {
                'user_info': {},      # 用户基本信息
                'contact_info': {},   # 联系方式信息
                'system_info': {},    # 系统信息
                'timestamp_info': {}  # 时间戳信息
            }

            self.connection.create_table(self.hbase_table_name, families)
            logger.info(f"✅ 创建HBase表: {self.hbase_table_name}")

            self.table = self.connection.table(self.hbase_table_name)
            return True

        except Exception as e:
            logger.error(f"❌ 设置HBase表失败: {e}")
            return False

    def get_pg_table_structure(self, conn):
        """获取PostgreSQL表结构"""
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'user_info_base'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()

            logger.info("📊 PostgreSQL表结构:")
            for column in columns:
                logger.info(f"  - {column[0]}: {column[1]}")

            return [col[0] for col in columns]
        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            return []

    def get_pg_data(self, conn, batch_size=1000):
        """从PostgreSQL获取数据"""
        try:
            cursor = conn.cursor()

            # 获取总记录数
            cursor.execute("SELECT COUNT(*) FROM user_info_base")
            total_count = cursor.fetchone()[0]
            logger.info(f"📊 PostgreSQL总记录数: {total_count}")

            # 分批获取数据
            offset = 0
            while True:
                query = """
                SELECT * FROM user_info_base 
                ORDER BY user_id
                LIMIT %s OFFSET %s
                """

                cursor.execute(query, (batch_size, offset))
                records = cursor.fetchall()

                if not records:
                    break

                yield records
                offset += batch_size

        except Exception as e:
            logger.error(f"❌ 获取PostgreSQL数据失败: {e}")
            yield []

    def generate_row_key(self, user_id, created_time=None):
        """生成HBase行键"""
        try:
            if created_time:
                timestamp = int(created_time.timestamp())
                reverse_timestamp = 9999999999 - timestamp
                return f"{reverse_timestamp:010d}_{user_id}"
            else:
                return f"9999999999_{user_id}"
        except:
            return f"9999999999_{user_id}"

    def convert_to_hbase_data(self, record, column_names):
        """将PostgreSQL记录转换为HBase格式"""
        # 假设user_id是第一个字段
        user_id = record[0]

        # 尝试获取创建时间（如果有的话）
        created_time = None
        if 'created_time' in column_names:
            created_time_index = column_names.index('created_time')
            created_time = record[created_time_index]

        row_key = self.generate_row_key(user_id, created_time)

        def safe_value(value):
            """安全处理值"""
            if value is None:
                return ''
            if isinstance(value, datetime):
                return value.strftime('%Y-%m-%d %H:%M:%S')
            return str(value)

        # 构建HBase数据
        data = {}

        # 根据列名分类存储
        for i, column_name in enumerate(column_names):
            value = record[i]

            # 根据列名分类到不同的列族
            if column_name in ['user_id', 'username', 'real_name', 'gender', 'birthday', 'age']:
                family = 'user_info'
            elif column_name in ['email', 'phone', 'address', 'city', 'country']:
                family = 'contact_info'
            elif column_name in ['created_time', 'updated_time', 'last_login']:
                family = 'timestamp_info'
            else:
                family = 'system_info'

            column_key = f"{family}:{column_name}".encode('utf-8')
            data[column_key] = safe_value(value).encode('utf-8')

        # 添加导入时间戳
        data[b'timestamp_info:import_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode('utf-8')

        return row_key, data

    def import_data(self, batch_size=100):
        """主导入函数"""
        logger.info("🚀 开始从PostgreSQL导入数据到HBase...")

        # 连接HBase
        if not self.connect_hbase():
            return False

        # 设置HBase表
        if not self.setup_hbase_table():
            return False

        # 连接PostgreSQL
        pg_conn = self.connect_postgresql()
        if not pg_conn:
            return False

        try:
            # 获取表结构
            column_names = self.get_pg_table_structure(pg_conn)
            if not column_names:
                logger.error("❌ 无法获取表结构")
                return False

            # 分批导入数据
            total_imported = 0
            batch_count = 0

            for batch_records in self.get_pg_data(pg_conn, batch_size):
                if not batch_records:
                    break

                batch_data = {}
                for record in batch_records:
                    row_key, hbase_data = self.convert_to_hbase_data(record, column_names)
                    batch_data[row_key] = hbase_data

                # 批量写入HBase
                if batch_data:
                    try:
                        with self.table.batch(batch_size=50) as batch:
                            for row_key, data in batch_data.items():
                                batch.put(row_key, data)

                        batch_count += 1
                        total_imported += len(batch_records)
                        logger.info(f"✅ 第 {batch_count} 批数据导入成功: {len(batch_records)} 条记录")

                    except Exception as e:
                        logger.error(f"❌ 批量插入失败: {e}")
                        # 尝试单条插入
                        success_count = 0
                        for row_key, data in batch_data.items():
                            try:
                                self.table.put(row_key, data)
                                success_count += 1
                            except Exception as single_error:
                                logger.error(f"单条插入失败 {row_key}: {single_error}")

                        total_imported += success_count
                        logger.info(f"单条插入完成，成功 {success_count}/{len(batch_records)} 条")

            logger.info(f"🎉 数据导入完成！总计导入 {total_imported} 条记录到HBase")
            return True

        finally:
            pg_conn.close()
            if self.connection:
                self.connection.close()

    def verify_import(self, sample_size=3):
        """验证导入结果"""
        try:
            self.connect_hbase()
            self.table = self.connection.table(self.hbase_table_name)

            logger.info(f"🔍 验证导入结果 (显示前{sample_size}条记录):")

            count = 0
            for key, data in self.table.scan(limit=sample_size):
                print(f"\n{'='*50}")
                print(f"Row Key: {key.decode('utf-8')}")
                print(f"{'='*50}")

                for column, value in data.items():
                    family, qualifier = column.split(b':')
                    print(f"  {family.decode('utf-8')}.{qualifier.decode('utf-8')}: {value.decode('utf-8')}")

                count += 1
                if count >= sample_size:
                    break

            # 获取总行数
            row_count = sum(1 for _ in self.table.scan())
            logger.info(f"📊 HBase表中总记录数: {row_count}")

            self.connection.close()

        except Exception as e:
            logger.error(f"验证失败: {e}")

def main():
    """主函数"""
    importer = PGToHBaseImporter()

    # 执行导入
    if importer.import_data(batch_size=100):
        # 验证结果
        importer.verify_import(sample_size=3)
    else:
        logger.error("❌ 数据导入失败")

if __name__ == "__main__":
    main()