# Satfeeds 推流记录

查询阿里云历史推流：一条流一行，按开始推流时间筛选。仓库：[cpmmobi/satfeeds](https://github.com/cpmmobi/satfeeds)。

## 本地启动

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
# 填入阿里云 AccessKey
./.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8787
```

浏览器打开 http://127.0.0.1:8787

密钥放在 `.env`，不要提交到 git。

## Vercel

导入 GitHub 仓库后，在 Vercel 项目 **Environment Variables** 里配置：

- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
- `LIVE_DOMAIN`（默认 `pushn.fheuuw.com`）
- `LIVE_APP`（默认 `sla`）
- `LIVE_REGION`（默认 `ap-southeast-1`）
- `STREAM_KEYWORD`（默认 `Satfeeds`）
- `MATCH_API_URL`（默认 `https://api.sla.homes/lives/getMatches`）
- `TEST_PUSH_DOMAIN`（测试推流域名，默认 `pushn.fheuuw.com`）
- `TEST_PUSH_AUTH_KEY`（测试推流鉴权密钥）
- `TEST_APP_NAME`（默认 `Satfeeds`）
- `TEST_PLAY_DOMAIN`（测试播放域名，默认 `trial.sla.homes`）
- `TEST_PLAY_AUTH_KEY`（测试播放鉴权密钥）

测试推流页：`/test`，提供 test1–test5（App `Satfeeds`），不要往正式 App `sla` 推测试流。

查询会打阿里云接口，时间范围较大时可能接近函数超时。Hobby 默认较短，需要更长超时可升级计划或在 `vercel.json` 里调整 `maxDuration`。

## RAM 权限

请给该 RAM 用户添加系统策略 **AliyunLiveReadOnlyAccess**，或自定义：

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "live:DescribeLiveStreamsPublishList",
        "live:DescribeLiveStreamsOnlineList",
        "live:DescribeLiveStreamState",
        "live:DescribeLiveStreamDetailFrameRateAndBitRateData",
        "live:DescribeLiveStreamBitRateData"
      ],
      "Resource": "*"
    }
  ]
}
```

添加后等 1 分钟再生效。

阿里云实际可查范围按近 **20 天**、单次跨度不超过 **20 天** 限制（官方文档写 30 天，实测更早会失败或超时）。每页最多 3000 条，单用户 QPS 3 次/秒。
