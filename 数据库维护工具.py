"""
数据库维护脚本 - 清理和优化数据库
"""
import sqlite3
import os
from datetime import datetime, timedelta

def vacuum_database(db_path):
    """压缩数据库，回收空间"""
    print(f"正在压缩数据库: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.execute("VACUUM")
    conn.close()
    print("压缩完成")

def cleanup_old_logs(db_path, keep_days=30):
    """清理旧日志，保留最近N天"""
    print(f"清理 {keep_days} 天前的日志...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 计算删除前的记录数
    cursor.execute("SELECT COUNT(*) FROM chat_logs")
    before_count = cursor.fetchone()[0]

    # 删除旧记录
    cutoff_date = datetime.now() - timedelta(days=keep_days)
    cursor.execute(
        "DELETE FROM chat_logs WHERE timestamp < datetime(?)",
        (cutoff_date.strftime('%Y-%m-%d %H:%M:%S'),)
    )

    # 计算删除后的记录数
    cursor.execute("SELECT COUNT(*) FROM chat_logs")
    after_count = cursor.fetchone()[0]

    conn.commit()
    conn.close()

    deleted = before_count - after_count
    print(f"删除了 {deleted} 条记录 (保留 {after_count} 条)")

def add_auto_cleanup_trigger(db_path, max_records=10000):
    """添加自动清理触发器：当记录数超过阈值时自动删除最旧的"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 删除旧触发器（如果存在）
    cursor.execute("DROP TRIGGER IF EXISTS auto_cleanup_old_logs")

    # 创建新触发器
    cursor.execute(f"""
        CREATE TRIGGER IF NOT EXISTS auto_cleanup_old_logs
        AFTER INSERT ON chat_logs
        BEGIN
            DELETE FROM chat_logs
            WHERE id IN (
                SELECT id FROM chat_logs
                ORDER BY timestamp ASC
                LIMIT MAX(0, (SELECT COUNT(*) FROM chat_logs) - {max_records})
            );
        END;
    """)

    conn.commit()
    conn.close()
    print(f"已设置自动清理触发器：最多保留 {max_records} 条记录")

def get_db_size(db_path):
    """获取数据库文件大小"""
    if os.path.exists(db_path):
        size_bytes = os.path.getsize(db_path)
        size_mb = size_bytes / (1024 * 1024)
        return f"{size_mb:.2f} MB"
    return "不存在"

if __name__ == "__main__":
    chat_db = "chat_logs.db"
    token_db = "token_stats.db"

    print("=" * 50)
    print("数据库维护工具")
    print("=" * 50)

    # 显示当前大小
    print(f"\n当前数据库大小:")
    print(f"  chat_logs.db: {get_db_size(chat_db)}")
    print(f"  token_stats.db: {get_db_size(token_db)}")

    print("\n请选择操作:")
    print("1. 清理30天前的日志")
    print("2. 清理60天前的日志")
    print("3. 仅保留最近10000条记录")
    print("4. 压缩数据库（VACUUM）")
    print("5. 添加自动清理触发器")
    print("6. 全部执行（推荐）")

    choice = input("\n输入选项 (1-6): ").strip()

    if choice == "1":
        cleanup_old_logs(chat_db, keep_days=30)
        vacuum_database(chat_db)
    elif choice == "2":
        cleanup_old_logs(chat_db, keep_days=60)
        vacuum_database(chat_db)
    elif choice == "3":
        conn = sqlite3.connect(chat_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM chat_logs")
        total = cursor.fetchone()[0]
        if total > 10000:
            cursor.execute("""
                DELETE FROM chat_logs
                WHERE id IN (
                    SELECT id FROM chat_logs
                    ORDER BY timestamp ASC
                    LIMIT ?
                )
            """, (total - 10000,))
            conn.commit()
            print(f"已删除 {total - 10000} 条旧记录")
        conn.close()
        vacuum_database(chat_db)
    elif choice == "4":
        vacuum_database(chat_db)
        vacuum_database(token_db)
    elif choice == "5":
        add_auto_cleanup_trigger(chat_db, max_records=10000)
    elif choice == "6":
        print("\n执行完整维护流程...")
        cleanup_old_logs(chat_db, keep_days=30)
        add_auto_cleanup_trigger(chat_db, max_records=10000)
        vacuum_database(chat_db)
        vacuum_database(token_db)
    else:
        print("无效选项")
        exit(1)

    # 显示优化后大小
    print(f"\n优化后数据库大小:")
    print(f"  chat_logs.db: {get_db_size(chat_db)}")
    print(f"  token_stats.db: {get_db_size(token_db)}")
    print("\n✓ 维护完成")
