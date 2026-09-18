# Satfeeds 比赛信息批量查询接口

## 用途

根据比赛 ID 批量返回比赛名称、开赛时间、结束时间，用于对照阿里云推流记录，判断赛中是否断流。只需覆盖近 30 天，不必做长期历史库。

## 比赛 ID

阿里云推流名格式：`{前缀}-{比赛ID}-Satfeeds`

例如：`1-6a886f1ffbc4c8ef09351ae4-Satfeeds`

其中 `6a886f1ffbc4c8ef09351ae4` 即为比赛 ID。接口按这个 ID 查询。

## 接口

- Method：`POST`
- Path：`/api/v1/matches/batch`
- Content-Type：`application/json`
- 鉴权：按你们现有方式（Header 里带 Token / API Key 即可）

## 请求

```json
{
  "ids": [
    "6a886f1ffbc4c8ef09351ae4",
    "6a886f1ffbc4c8ef09351ae6"
  ]
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| ids | string[] | 是 | 比赛 ID 列表，单次最多 200 个，去重后查询 |

## 成功响应 `200`

```json
{
  "items": [
    {
      "id": "6a886f1ffbc4c8ef09351ae4",
      "match_name": "英超 阿森纳 vs 切尔西",
      "league": "英超",
      "home": "阿森纳",
      "away": "切尔西",
      "start_time": "2026-09-15T17:00:00+08:00",
      "end_time": "2026-09-15T18:55:00+08:00"
    }
  ],
  "not_found": [
    "6a886f1ffbc4c8ef09351ae6"
  ]
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| id | string | 是 | 比赛 ID，与请求一致 |
| match_name | string | 是 | 展示用全称 |
| league | string | 否 | 联赛/赛事 |
| home | string | 否 | 主队 |
| away | string | 否 | 客队 |
| start_time | string | 是 | 开赛时间，ISO 8601，带时区 |
| end_time | string | 是 | 完赛时间，ISO 8601，带时区。未结束则返回 `null` |

`items` 只包含查到的比赛；查不到的放进 `not_found`，不要省略这个数组。

## 规则

1. 批量查询，一次请求返回全部结果，不要拆成多次单条。
2. 近 30 天内的比赛必须能查到；更早的查不到时放进 `not_found` 即可。
3. `end_time` 尽量给真实完赛时间。没有精确完赛时间时，可用官方完赛/关播时间；仍没有则 `null`，不要用开赛时间凑。
4. 时间统一带时区，建议 `+08:00`。
5. 同一 ID 出现多次，只返回一条。
6. `ids` 为空或超过 200 个时返回 `400`。

## 错误

```json
{
  "error": "ids_too_many",
  "message": "单次最多查询 200 个比赛 ID"
}
```
