# verify_odbc18.py
import pyodbc

def check_odbc18():
    print("=== 验证 ODBC Driver 18 安装 ===")

    drivers = pyodbc.drivers()
    print("📋 当前系统所有ODBC驱动:")

    found_odbc18 = False
    for i, driver in enumerate(drivers, 1):
        print(f"   {i}. {driver}")
        if '18' in driver or 'ODBC Driver 18' in driver:
            found_odbc18 = True
            print(f"      ✅ 找到 ODBC Driver 18!")

    return found_odbc18

def test_odbc18_connection():
    print("\n=== 测试 ODBC 18 连接 ===")

    # ODBC 18 的连接配置
    connection_attempts = [
        {
            'name': '标准加密连接',
            'conn_str': "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.200.102,1433;DATABASE=realtime_v3;UID=sa;PWD=Xy0511./;TrustServerCertificate=yes;Encrypt=yes;"
        },
        {
            'name': '可选加密',
            'conn_str': "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.200.102,1433;DATABASE=realtime_v3;UID=sa;PWD=Xy0511./;TrustServerCertificate=yes;Encrypt=optional;"
        },
        {
            'name': '无加密',
            'conn_str': "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.200.102,1433;DATABASE=realtime_v3;UID=sa;PWD=Xy0511./;TrustServerCertificate=yes;Encrypt=no;"
        }
    ]

    for attempt in connection_attempts:
        print(f"\n尝试: {attempt['name']}")
        try:
            conn = pyodbc.connect(attempt['conn_str'], timeout=10)
            cursor = conn.cursor()

            cursor.execute("SELECT @@VERSION as version")
            version_row = cursor.fetchone()
            print(f"✅ 连接成功!")
            print(f"   SQL Server版本: {version_row.version[:60]}...")

            cursor.execute("SELECT DB_NAME() as db_name")
            db_row = cursor.fetchone()
            print(f"   当前数据库: {db_row.db_name}")

            conn.close()
            return attempt['conn_str']

        except Exception as e:
            print(f"❌ 失败: {e}")

    return None

if __name__ == "__main__":
    if check_odbc18():
        success_conn = test_odbc18_connection()
        if success_conn:
            print(f"\n🎉 ODBC 18 连接测试成功!")
            print(f"使用此连接字符串: {success_conn}")
        else:
            print(f"\n💡 连接测试失败，请检查SQL Server配置")
    else:
        print(f"\n❌ 未找到 ODBC Driver 18")