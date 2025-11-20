import pandas as pd

from src.stream_mock_data import sink_dict_2postgresql, logger


def insert_user_info_from_csv():
    """从CSV文件读取数据并插入到user_info_base表"""
    try:
        # 读取CSV文件
        df = pd.read_csv('spider_db_public_user_info_base (1).csv')

        # 转换数据格式
        users = []
        for _, row in df.iterrows():
            user = {
                "id": row['id'],
                "user_id": row['user_id'],
                "uname": row['uname'],
                "phone_num": row['phone_num'],
                "birthday": row['birthday'],
                "gender": row['gender'],
                "address": row['address'],
                "ts": row['ts']
            }
            users.append(user)

        # 插入到数据库
        sink_dict_2postgresql(
            sink_data=users,
            table_name='user_info_base',
            sink_fields=['id', 'user_id', 'uname', 'phone_num', 'birthday', 'gender', 'address', 'ts']
        )

        logger.info(f"成功从CSV文件插入 {len(users)} 条用户数据")

    except Exception as e:
        logger.error(f"从CSV文件插入数据时发生错误: {e}")

# 调用函数
insert_user_info_from_csv()