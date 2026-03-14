import csv
from collections import defaultdict


def count_mora(text):
    """ひらがなの音数をカウントする"""
    small_kana = 'ぁぃぅぇぉゃゅょゎァィゥェォャュョヮ'
    count = 0
    for char in text:
        if char not in small_kana and char.strip():
            count += 1
    return count


def format_tanka_result(phrases, stations, station_kana):
    """見つかった駅経路のリストのリストを文字列化する"""
    tanka_name_parts = []
    tanka_kana_parts = []

    for phrase in phrases:
        names = [stations[cd] for cd in phrase]
        kanas = [station_kana[cd] for cd in phrase]
        tanka_name_parts.append("・".join(names))
        tanka_kana_parts.append("".join(kanas))

    tanka_name_str = "　".join(tanka_name_parts)
    tanka_kana_str = "　".join(tanka_kana_parts)

    return tanka_name_str, tanka_kana_str


def main():
    stations = {}
    station_kana = {}
    station_mora = {}

    with open('./data/station_with_kana_modified.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cd = row['station_cd']
            name = row['station_name']
            kana = row['station_name_hiragana']

            # 読み仮名が取得できていない駅はスキップする
            if not kana:
                continue

            stations[cd] = name
            station_kana[cd] = kana
            station_mora[cd] = count_mora(kana)

    graph = defaultdict(set)
    with open('./data/eki-data/join20260226.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cd1 = row['station_cd1']
            cd2 = row['station_cd2']
            if cd1 in stations and cd2 in stations:
                graph[cd1].add(cd2)
                graph[cd2].add(cd1)

    results = []

    # 短歌の各句で許容される音数
    TARGETS = [
        [5, 6],  # 初句 (5音 または 6音)
        [7],     # 二句 (7音)
        [5, 6],  # 三句 (5音 または 6音)
        [7, 8],  # 四句 (7音 または 8音)
        [7]      # 結句 (7音)
    ]

    def dfs(current_cd, visited, phrases, phrase_idx, current_mora):
        max_allowed = max(TARGETS[phrase_idx])

        if current_mora > max_allowed:
            return

        if current_mora in TARGETS[phrase_idx]:
            if phrase_idx == 4:
                results.append([list(p) for p in phrases])
            else:
                phrases.append([])
                for neighbor in graph[current_cd]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        phrases[-1].append(neighbor)
                        dfs(neighbor, visited, phrases,
                            phrase_idx + 1, station_mora[neighbor])
                        phrases[-1].pop()
                        visited.remove(neighbor)
                phrases.pop()

        if current_mora < max_allowed:
            for neighbor in graph[current_cd]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    phrases[-1].append(neighbor)
                    dfs(neighbor, visited, phrases, phrase_idx,
                        current_mora + station_mora[neighbor])
                    phrases[-1].pop()
                    visited.remove(neighbor)

    for start_cd in stations.keys():
        start_mora = station_mora[start_cd]
        if start_mora <= max(TARGETS[0]):
            dfs(start_cd, {start_cd}, [[start_cd]], 0, start_mora)

    print(f"\n探索完了。経路が全 {len(results)} 件見つかりました。\n")

    grouped_results = defaultdict(list)
    for phrases in results:
        mora_counts = tuple(sum(station_mora[cd]
                            for cd in phrase) for phrase in phrases)
        grouped_results[mora_counts].append(phrases)

    def sort_key(counts):
        """
        グループの表示順を決めるためのソートキーを生成する
        """
        # 標準の音数
        STANDARD = (5, 7, 5, 7, 7)

        # ルール1: 音数の総和が31に近いほど優先度が高い（絶対値で比較）
        diff_from_31 = abs(sum(counts) - 31)

        # ルール2: 句の前半の字余り・字足らずを優先する
        # 標準との差（ズレの大きさ）をマイナス値で評価することで、
        # 前の句でズレが大きいグループほどソート順が先（昇順で上）に来るようにする
        deviations = [-abs(c - s) for c, s in zip(counts, STANDARD)]

        # 優先順位に従ってタプルを返す
        return (diff_from_31, *deviations)

    sorted_counts = sorted(grouped_results.keys(), key=sort_key)

    for counts in sorted_counts:
        group_phrases = grouped_results[counts]
        mora_str = "-".join(map(str, counts))

        print("=" * 60)
        print(f"【{mora_str}】({len(group_phrases)}件)")
        print("=" * 60)

        for phrases in group_phrases:
            tanka_name, tanka_kana = format_tanka_result(
                phrases, stations, station_kana)
            print(f"  {tanka_name}")
            print(f"  {tanka_kana}")
            print()


if __name__ == '__main__':
    main()
