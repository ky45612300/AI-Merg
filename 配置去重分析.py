"""
配置去重工具 - 分析和清理重复的端点配置
"""
import json
from collections import defaultdict

def analyze_config(config_path="api_config.json"):
    """分析配置文件，找出重复项"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    endpoints = config.get("api_endpoints", [])

    # 按名称和模型分组
    groups = defaultdict(list)
    for ep in endpoints:
        key = (ep.get("name"), ep.get("model"), ep.get("base_url"))
        groups[key].append(ep)

    print("=" * 60)
    print("配置文件分析报告")
    print("=" * 60)
    print(f"\n总端点数: {len(endpoints)}")
    print(f"启用端点: {sum(1 for ep in endpoints if ep.get('enabled'))}")
    print(f"禁用端点: {sum(1 for ep in endpoints if not ep.get('enabled'))}")

    # 按名称统计
    name_count = defaultdict(int)
    for ep in endpoints:
        name_count[ep.get("name", "未命名")] += 1

    print(f"\n按服务商统计:")
    for name, count in sorted(name_count.items(), key=lambda x: -x[1]):
        print(f"  {name}: {count} 个端点")

    # 找出完全重复的
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    if duplicates:
        print(f"\n⚠️ 发现 {len(duplicates)} 组疑似重复配置:")
        for (name, model, url), eps in duplicates.items():
            print(f"\n  {name} - {model}")
            print(f"    数量: {len(eps)} 个")
            print(f"    ID: {[ep['id'][:8] for ep in eps]}")
            print(f"    优先级: {[ep.get('priority') for ep in eps]}")

    # 按优先级统计
    priority_dist = defaultdict(int)
    for ep in endpoints:
        priority_dist[ep.get("priority", 999)] += 1

    print(f"\n按优先级分布:")
    for priority in sorted(priority_dist.keys()):
        count = priority_dist[priority]
        bar = "█" * (count // 2)
        print(f"  优先级 {priority}: {count:2d} 个 {bar}")

    return endpoints, duplicates

def suggest_optimization(endpoints):
    """建议优化方案"""
    print("\n" + "=" * 60)
    print("优化建议")
    print("=" * 60)

    # 1. 禁用的端点
    disabled = [ep for ep in endpoints if not ep.get("enabled")]
    if disabled:
        print(f"\n1. 清理 {len(disabled)} 个已禁用端点")
        print("   这些端点不会被使用，建议删除以简化配置")

    # 2. 相同优先级过多
    from collections import Counter
    priority_count = Counter(ep.get("priority") for ep in endpoints if ep.get("enabled"))
    if any(count > 10 for count in priority_count.values()):
        print(f"\n2. 优化优先级分配")
        print("   部分优先级组端点过多，建议重新分级")
        for priority, count in priority_count.most_common(5):
            if count > 10:
                print(f"   - 优先级 {priority}: {count} 个端点（建议拆分）")

    # 3. 超时设置建议
    timeout_values = [ep.get("timeout") for ep in endpoints if ep.get("enabled")]
    avg_timeout = sum(timeout_values) / len(timeout_values) if timeout_values else 0
    print(f"\n3. 超时配置")
    print(f"   平均超时: {avg_timeout:.1f}秒")
    if avg_timeout > 100:
        print("   ⚠️ 平均超时较高，可能影响响应速度")

    # 4. 冷却时间建议
    cooldown_values = [ep.get("cooldown_minutes") for ep in endpoints if ep.get("enabled")]
    avg_cooldown = sum(cooldown_values) / len(cooldown_values) if cooldown_values else 0
    print(f"\n4. 冷却时间")
    print(f"   平均冷却: {avg_cooldown:.1f}分钟")
    if avg_cooldown > 20:
        print("   ℹ️ 冷却时间较长，故障切换后恢复较慢")

def interactive_cleanup():
    """交互式清理"""
    endpoints, duplicates = analyze_config()

    if not duplicates:
        print("\n✓ 没有发现明显重复的配置")
        suggest_optimization(endpoints)
        return

    print("\n" + "=" * 60)
    print("是否要生成清理建议？(y/n)")
    if input().lower() != 'y':
        return

    print("\n建议删除以下重复端点（保留优先级最高的）:")
    to_remove = []
    for (name, model, url), eps in duplicates.items():
        # 保留优先级最高的
        eps_sorted = sorted(eps, key=lambda x: (x.get("priority", 999), -x.get("_total_calls", 0)))
        keep = eps_sorted[0]
        remove = eps_sorted[1:]

        print(f"\n{name} - {model}:")
        print(f"  保留: {keep['id'][:8]} (优先级 {keep.get('priority')})")
        for ep in remove:
            print(f"  删除: {ep['id'][:8]} (优先级 {ep.get('priority')})")
            to_remove.append(ep['id'])

    print(f"\n共建议删除 {len(to_remove)} 个端点")
    print("注意：这只是建议，请根据实际使用情况决定")

if __name__ == "__main__":
    try:
        interactive_cleanup()
    except FileNotFoundError:
        print("错误: 找不到 api_config.json 文件")
    except json.JSONDecodeError:
        print("错误: api_config.json 格式不正确")
