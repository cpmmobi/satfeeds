from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from alibabacloud_live20161101.client import Client as LiveClient
from alibabacloud_live20161101.models import (
    DescribeLiveStreamBitRateDataRequest,
    DescribeLiveStreamDetailFrameRateAndBitRateDataRequest,
    DescribeLiveStreamStateRequest,
    DescribeLiveStreamsOnlineListRequest,
    DescribeLiveStreamsPublishListRequest,
)
from alibabacloud_tea_openapi.models import Config
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=True)

TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
KEYWORD = os.getenv("STREAM_KEYWORD", "Satfeeds")
DOMAIN = os.getenv("LIVE_DOMAIN", "pushn.fheuuw.com")
APP_NAME = os.getenv("LIVE_APP", "sla")
REGION = os.getenv("LIVE_REGION", "ap-southeast-1")
MAX_SPAN_DAYS = 20
MAX_HISTORY_DAYS = 20
PAGE_SIZE = 3000
PAGE_INTERVAL_SEC = 0.4
MATCH_API_URL = os.getenv("MATCH_API_URL", "https://api.sla.homes/lives/getMatches")
MATCH_BATCH_SIZE = 200
SPORT_LABEL = {1: "足球", 2: "篮球"}
DEFAULT_MATCH_DURATION = timedelta(minutes=110)
MAX_MATCH_DURATION = timedelta(hours=4)
LONG_INTERRUPT_SECONDS = 60
CRITICAL_DISCONNECTS = 3
TEST_PUSH_DOMAIN = os.getenv("TEST_PUSH_DOMAIN", "pushn.fheuuw.com").strip()
TEST_PUSH_KEY = os.getenv("TEST_PUSH_AUTH_KEY", "").strip()
TEST_APP_NAME = os.getenv("TEST_APP_NAME", "Satfeeds").strip() or "Satfeeds"
TEST_STREAM_NAMES = ["test1", "test2", "test3", "test4", "test5"]
TEST_PLAY_DOMAIN = os.getenv("TEST_PLAY_DOMAIN", "trial.sla.homes").strip()
TEST_PLAY_KEY = os.getenv("TEST_PLAY_AUTH_KEY", "").strip()
TEST_AUTH_HOURS = 24
TEST_METRIC_MAX_HOURS = 6
TEST_METRIC_CHUNK = timedelta(hours=1)

app = FastAPI(title="Satfeeds 推流记录")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")


def live_client() -> LiveClient:
    ak = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "").strip()
    sk = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "").strip()
    if not ak or not sk:
        raise HTTPException(status_code=500, detail="未配置阿里云 AccessKey，请检查 .env")
    cfg = Config(access_key_id=ak, access_key_secret=sk, endpoint="live.aliyuncs.com")
    cfg.region_id = REGION
    cfg.connect_timeout = 10000
    cfg.read_timeout = 60000
    return LiveClient(cfg)


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00").replace(" ", "T", 1)
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(UTC)


def fmt_cst(dt: datetime | None, live: bool = False) -> str:
    if live or dt is None:
        return "推流中"
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    sec = int(round(seconds))
    sign = "-" if sec < 0 else ""
    sec = abs(sec)
    hours, rem = divmod(sec, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{sign}{hours}小时{minutes}分{secs}秒"
    if minutes:
        return f"{sign}{minutes}分{secs}秒"
    return f"{sign}{secs}秒"


def to_utc_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_match_end(start: datetime | None, end: datetime | None) -> datetime | None:
    if start is None or end is None:
        return None
    if end <= start:
        return None
    duration = end - start
    if duration < timedelta(minutes=60) or duration > MAX_MATCH_DURATION:
        return None
    return end


def parse_stream_identity(stream_id: str) -> tuple[str | None, int | None]:
    parts = stream_id.split("-")
    if len(parts) < 3 or parts[-1].lower() != "satfeeds":
        return None, None
    prefix, match_id = parts[0], parts[1]
    sport_type = int(prefix) if prefix.isdigit() and int(prefix) in SPORT_LABEL else None
    return match_id, sport_type


def fetch_matches_for_type(sport_type: int, ids: list[str]) -> dict[str, dict[str, Any]]:
    unique: list[str] = []
    seen: set[str] = set()
    for match_id in ids:
        if match_id and match_id not in seen:
            seen.add(match_id)
            unique.append(match_id)
    result: dict[str, dict[str, Any]] = {}
    if not unique:
        return result
    for offset in range(0, len(unique), MATCH_BATCH_SIZE):
        chunk = unique[offset : offset + MATCH_BATCH_SIZE]
        req = urllib.request.Request(
            MATCH_API_URL,
            data=json.dumps({"type": sport_type, "ids": chunk}).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
            continue
        for item in payload.get("items") or []:
            match_id = item.get("id")
            if match_id:
                row = dict(item)
                row["sport_type"] = sport_type
                result[str(match_id)] = row
    return result


def fetch_matches(streams: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[int, list[str]] = defaultdict(list)
    unknown: list[str] = []
    for stream in streams:
        match_id, sport_type = parse_stream_identity(stream["stream_id"])
        if not match_id:
            continue
        if sport_type:
            grouped[sport_type].append(match_id)
        else:
            unknown.append(match_id)
    mapping: dict[str, dict[str, Any]] = {}
    for sport_type, ids in grouped.items():
        mapping.update(fetch_matches_for_type(sport_type, ids))
    leftover = [match_id for match_id in unknown if match_id not in mapping]
    if leftover:
        mapping.update(fetch_matches_for_type(1, leftover))
        leftover = [match_id for match_id in leftover if match_id not in mapping]
        if leftover:
            mapping.update(fetch_matches_for_type(2, leftover))
    return mapping


def attach_matches(streams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = fetch_matches(streams)
    for stream in streams:
        match_id, sport_type = parse_stream_identity(stream["stream_id"])
        info = mapping.get(match_id or "") or {}
        resolved_type = info.get("sport_type") or sport_type
        start_raw = info.get("start_time")
        end_raw = info.get("end_time")
        start_dt = parse_iso(start_raw) if start_raw else None
        end_dt = sanitize_match_end(start_dt, parse_iso(end_raw) if end_raw else None)
        stream["match_id"] = match_id or ""
        stream["sport"] = SPORT_LABEL.get(int(resolved_type), "") if resolved_type else ""
        stream["match_name"] = info.get("match_name") or "未知比赛"
        stream["league"] = info.get("league") or ""
        stream["match_start"] = fmt_cst(start_dt) if start_dt else "—"
        stream["match_end"] = fmt_cst(end_dt) if end_dt else "—"
        stream["match_end_assumed"] = bool(start_dt) and end_dt is None
        stream["_match_start"] = start_dt
        stream["_match_end"] = end_dt
    return streams


def fetch_sessions(start: datetime, end: datetime) -> list[dict[str, Any]]:
    client = live_client()
    records: list[dict[str, Any]] = []
    page = 1
    total_page = 1
    while page <= total_page:
        req = DescribeLiveStreamsPublishListRequest(
            domain_name=DOMAIN,
            app_name=APP_NAME or None,
            stream_name=KEYWORD,
            start_time=to_utc_z(start),
            end_time=to_utc_z(end),
            page_size=PAGE_SIZE,
            page_number=page,
            query_type="fuzzy",
            stream_type="raw",
            order_by="publish_time_desc",
            region_id=REGION,
        )
        try:
            resp = None
            last_error = None
            for attempt in range(3):
                try:
                    resp = client.describe_live_streams_publish_list(req)
                    break
                except Exception as exc:
                    last_error = exc
                    message = str(exc)
                    if "QpsOverLimit" in message and attempt < 2:
                        time.sleep(1.0)
                        continue
                    raise
            if resp is None:
                raise last_error or RuntimeError("阿里云接口调用失败")
        except Exception as exc:
            message = str(exc)
            if "Forbidden" in message or "401" in message:
                raise HTTPException(
                    status_code=403,
                    detail=(
                        "阿里云拒绝了历史流查询。请给这个 AccessKey 所属 RAM 用户添加系统策略 "
                        "AliyunLiveReadOnlyAccess，或自定义允许 live:DescribeLiveStreamsPublishList。"
                        "当前密钥能读取域名，但不能读取推流记录。"
                    ),
                ) from exc
            raise HTTPException(status_code=502, detail=f"阿里云接口调用失败：{message[:300]}") from exc

        body = resp.body
        total_page = int(body.total_page or 1)
        infos = []
        if body.publish_info and body.publish_info.live_stream_publish_info:
            infos = body.publish_info.live_stream_publish_info
        for item in infos:
            name = item.stream_name or ""
            if KEYWORD.lower() not in name.lower():
                continue
            records.append(
                {
                    "stream_id": name,
                    "publish_time": parse_iso(item.publish_time),
                    "stop_time": parse_iso(item.stop_time),
                    "ip": item.client_addr or "",
                }
            )
        page += 1
        if page <= total_page:
            time.sleep(PAGE_INTERVAL_SEC)
    return records


def aggregate(sessions: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sessions:
        pub = row["publish_time"]
        if pub is None or pub < start or pub >= end:
            continue
        grouped[row["stream_id"]].append(row)

    result = []
    for stream_id, items in grouped.items():
        items.sort(key=lambda x: x["publish_time"] or datetime.min.replace(tzinfo=UTC))
        start_dt = items[0]["publish_time"]
        stop_times = [x["stop_time"] for x in items if x["stop_time"]]
        still_live = any(x["stop_time"] is None for x in items)
        end_dt = None if still_live else (max(stop_times) if stop_times else None)
        total_seconds = 0.0
        ips: list[str] = []
        details: list[dict[str, Any]] = []
        prev_stop: datetime | None = None
        now = datetime.now(UTC)
        for index, item in enumerate(items, start=1):
            pub = item["publish_time"]
            stop = item["stop_time"]
            live_seg = stop is None
            end_seg = now if live_seg else stop
            duration = (end_seg - pub).total_seconds() if pub and end_seg else None
            if duration is not None and duration > 0:
                total_seconds += duration
            gap = (pub - prev_stop).total_seconds() if pub and prev_stop else None
            if item["ip"] and item["ip"] not in ips:
                ips.append(item["ip"])
            details.append(
                {
                    "index": index,
                    "start_time": fmt_cst(pub),
                    "end_time": fmt_cst(stop, live=live_seg),
                    "duration": fmt_duration(duration),
                    "gap": "—" if index == 1 else fmt_duration(gap),
                    "ip": item["ip"] or "—",
                    "counted": False,
                }
            )
            if stop:
                prev_stop = stop
        result.append(
            {
                "stream_id": stream_id,
                "start_time": fmt_cst(start_dt),
                "end_time": fmt_cst(end_dt, live=still_live),
                "disconnects": max(len(items) - 1, 0),
                "publish_ip": "、".join(ips),
                "sessions": len(items),
                "effective_seconds": int(total_seconds),
                "stable_over_1h": False,
                "details": details,
                "_raw": items,
                "_sort": start_dt.timestamp() if start_dt else 0,
            }
        )
    result.sort(key=lambda x: x["_sort"], reverse=True)
    for row in result:
        row.pop("_sort", None)
    return result


def window_overlap_seconds(
    gap_start: datetime | None,
    gap_end: datetime | None,
    window_start: datetime,
    window_end: datetime,
) -> float:
    if not gap_start or not gap_end:
        return 0.0
    begin = max(gap_start, window_start)
    finish = min(gap_end, window_end)
    if finish <= begin:
        return 0.0
    return (finish - begin).total_seconds()


def classify_stream(disconnects: int | None, max_interrupt: float) -> tuple[str, list[str]]:
    if disconnects is None:
        return "", []
    reasons: list[str] = []
    if disconnects > CRITICAL_DISCONNECTS:
        reasons.append(f"断流{disconnects}次")
    if max_interrupt > LONG_INTERRUPT_SECONDS:
        reasons.append(f"赛中中断{fmt_duration(max_interrupt)}")
    if reasons:
        return "critical", reasons
    if disconnects >= 1:
        return "warn", []
    return "ok", []


def apply_match_window(streams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    for stream in streams:
        raw = stream.pop("_raw", []) or []
        match_start = stream.pop("_match_start", None)
        match_end = stream.pop("_match_end", None)
        details = stream.get("details") or []
        if not match_start:
            stream["disconnects"] = None
            stream["stable_over_1h"] = False
            stream["disconnect_scope"] = "unknown"
            stream["match_end_assumed"] = False
            stream["max_interrupt_seconds"] = None
            stream["severity"] = ""
            stream["issue_reasons"] = []
            stream["issue_reason"] = ""
            continue

        assumed = match_end is None
        window_start = match_start - timedelta(minutes=15)
        window_end = match_end or (match_start + DEFAULT_MATCH_DURATION)
        disconnects = 0
        window_seconds = 0.0
        max_interrupt = 0.0

        for item in raw:
            pub = item.get("publish_time")
            stop = item.get("stop_time") or now
            if not pub:
                continue
            begin = max(pub, window_start)
            finish = min(stop, window_end)
            if finish > begin:
                window_seconds += (finish - begin).total_seconds()

        for index in range(len(raw) - 1):
            stop = raw[index].get("stop_time")
            nxt = raw[index + 1].get("publish_time")
            overlap = window_overlap_seconds(stop, nxt, window_start, window_end)
            counted = overlap > 0
            if counted:
                disconnects += 1
                if overlap > max_interrupt:
                    max_interrupt = overlap
            if index + 1 < len(details):
                details[index + 1]["counted"] = counted
                details[index + 1]["interrupt_seconds"] = int(round(overlap)) if counted else 0
                details[index + 1]["long_interrupt"] = overlap > LONG_INTERRUPT_SECONDS

        last = raw[-1] if raw else None
        trailing = False
        if last and last.get("stop_time"):
            last_stop = last["stop_time"]
            if window_start <= last_stop < window_end:
                trailing = True
                disconnects += 1
                overlap = (window_end - last_stop).total_seconds()
                if overlap > max_interrupt:
                    max_interrupt = overlap
                if details:
                    details[-1]["interrupt_seconds"] = int(round(overlap))
                    details[-1]["long_interrupt"] = overlap > LONG_INTERRUPT_SECONDS
        if trailing and details:
            details[-1]["counted"] = True
            details[-1]["trailing"] = True

        severity, reasons = classify_stream(disconnects, max_interrupt)
        stream["disconnects"] = disconnects
        stream["effective_seconds"] = int(window_seconds)
        stream["stable_over_1h"] = disconnects == 0 and window_seconds > 3600
        stream["disconnect_scope"] = "assumed" if assumed else "match"
        stream["match_end_assumed"] = assumed
        stream["stat_window_end"] = fmt_cst(window_end)
        stream["max_interrupt_seconds"] = int(round(max_interrupt))
        stream["severity"] = severity
        stream["issue_reasons"] = reasons
        stream["issue_reason"] = "；".join(reasons)
    return streams


def clamp_query_window(start_dt: datetime, end_dt: datetime) -> tuple[datetime, datetime, list[str]]:
    now = datetime.now(UTC)
    notes: list[str] = []
    latest = now
    earliest = now - timedelta(days=MAX_HISTORY_DAYS) + timedelta(minutes=1)
    max_span = timedelta(days=MAX_SPAN_DAYS) - timedelta(seconds=1)

    if end_dt > latest:
        end_dt = latest
        notes.append("结束时间不能晚于当前时间，已调整到现在")
    if start_dt < earliest:
        start_dt = earliest
        notes.append(f"阿里云只能查询近 {MAX_HISTORY_DAYS} 天，已调整开始时间")
    if end_dt - start_dt > max_span:
        start_dt = max(end_dt - max_span, earliest)
        notes.append(f"单次查询跨度不能超过 {MAX_SPAN_DAYS} 天，已缩短范围")
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="有效查询时间范围为空，请选择近 20 天内、且结束时间晚于开始时间的区间")
    return start_dt, end_dt, notes


@app.get("/")
def index() -> FileResponse:
    return FileResponse(
        ROOT / "static" / "index.html",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/test")
def test_page() -> FileResponse:
    return FileResponse(
        ROOT / "static" / "test.html",
        headers={"Cache-Control": "no-store"},
    )


def aliyun_auth_key(uri: str, key: str, expire_ts: int) -> str:
    raw = f"{uri}-{expire_ts}-0-0-{key}"
    return f"{expire_ts}-0-0-{hashlib.md5(raw.encode('utf-8')).hexdigest()}"


def signed_live_url(scheme_host: str, uri: str, key: str, expire_ts: int) -> str:
    return f"{scheme_host}{uri}?auth_key={aliyun_auth_key(uri, key, expire_ts)}"


def to_cst_z(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


def require_test_stream(name: str | None) -> str:
    stream = (name or "test1").strip()
    if stream not in TEST_STREAM_NAMES:
        raise HTTPException(status_code=400, detail="StreamName 只能是 test1–test5")
    return stream


def aliyun_invoke(client: LiveClient, method: str, request: Any, label: str) -> Any:
    fn = getattr(client, method)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return fn(request)
        except Exception as exc:
            last_error = exc
            message = str(exc)
            if "QpsOverLimit" in message and attempt < 2:
                time.sleep(1.0)
                continue
            if "Forbidden" in message or "401" in message:
                raise HTTPException(
                    status_code=403,
                    detail=f"阿里云拒绝了{label}。请给这个 AccessKey 所属 RAM 用户添加系统策略 AliyunLiveReadOnlyAccess。",
                ) from exc
            raise HTTPException(status_code=502, detail=f"阿里云{label}失败：{message[:300]}") from exc
    raise HTTPException(status_code=502, detail=f"阿里云{label}失败：{last_error}")


def as_kbps(value: float | int | None) -> float | None:
    if value is None:
        return None
    number = float(value)
    if number <= 0:
        return None
    return round(number / 1000.0, 1) if number >= 10000 else round(number, 1)


def fmt_resolution(width: int | None, height: int | None) -> str | None:
    if width and height:
        return f"{int(width)}×{int(height)}"
    return None


def build_test_stream(name: str) -> dict[str, Any]:
    if not TEST_PUSH_KEY or not TEST_PLAY_KEY:
        raise HTTPException(status_code=500, detail="未配置测试推流/播放鉴权密钥，请检查 .env")
    expire_ts = int(time.time()) + TEST_AUTH_HOURS * 3600
    push_uri = f"/{TEST_APP_NAME}/{name}"
    hls_uri = f"/{TEST_APP_NAME}/{name}.m3u8"
    flv_uri = f"/{TEST_APP_NAME}/{name}.flv"
    return {
        "stream_name": name,
        "obs_server": f"rtmp://{TEST_PUSH_DOMAIN}/{TEST_APP_NAME}/",
        "obs_key": f"{name}?auth_key={aliyun_auth_key(push_uri, TEST_PUSH_KEY, expire_ts)}",
        "push_rtmp": signed_live_url(f"rtmp://{TEST_PUSH_DOMAIN}", push_uri, TEST_PUSH_KEY, expire_ts),
        "play_hls": signed_live_url(f"https://{TEST_PLAY_DOMAIN}", hls_uri, TEST_PLAY_KEY, expire_ts),
        "play_flv": signed_live_url(f"https://{TEST_PLAY_DOMAIN}", flv_uri, TEST_PLAY_KEY, expire_ts),
        "play_rtmp": signed_live_url(f"rtmp://{TEST_PLAY_DOMAIN}", push_uri, TEST_PLAY_KEY, expire_ts),
        "hours": TEST_AUTH_HOURS,
        "expire_at": fmt_cst(datetime.fromtimestamp(expire_ts, UTC)),
    }


def fetch_test_online(client: LiveClient, name: str) -> dict[str, Any] | None:
    resp = aliyun_invoke(
        client,
        "describe_live_streams_online_list",
        DescribeLiveStreamsOnlineListRequest(
            domain_name=TEST_PUSH_DOMAIN,
            app_name=TEST_APP_NAME,
            stream_name=name,
            query_type="strict",
            stream_type="raw",
            page_num=1,
            page_size=10,
            region_id=REGION,
        ),
        "在线流查询",
    )
    infos = []
    if resp.body and resp.body.online_info and resp.body.online_info.live_stream_online_info:
        infos = resp.body.online_info.live_stream_online_info
    for item in infos:
        if (item.stream_name or "") == name and (item.app_name or "") == TEST_APP_NAME:
            return {
                "online": True,
                "publish_time": fmt_cst(parse_iso(item.publish_time)),
                "client_ip": item.client_ip or "",
                "video_kbps": as_kbps(item.video_data_rate),
                "audio_kbps": as_kbps(item.audio_data_rate),
                "fps": item.frame_rate,
                "width": item.width,
                "height": item.height,
                "resolution": fmt_resolution(item.width, item.height),
            }
    return None


def fetch_test_state(client: LiveClient, name: str) -> str:
    resp = aliyun_invoke(
        client,
        "describe_live_stream_state",
        DescribeLiveStreamStateRequest(
            domain_name=TEST_PUSH_DOMAIN,
            app_name=TEST_APP_NAME,
            stream_name=name,
            region_id=REGION,
        ),
        "流状态查询",
    )
    return (resp.body.stream_state if resp.body else None) or "offline"


def fetch_test_sessions(client: LiveClient, name: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    lookback_start = start - timedelta(hours=24)
    resp = aliyun_invoke(
        client,
        "describe_live_streams_publish_list",
        DescribeLiveStreamsPublishListRequest(
            domain_name=TEST_PUSH_DOMAIN,
            app_name=TEST_APP_NAME,
            stream_name=name,
            start_time=to_utc_z(lookback_start),
            end_time=to_utc_z(end),
            page_size=PAGE_SIZE,
            page_number=1,
            query_type="fuzzy",
            stream_type="raw",
            order_by="publish_time_desc",
            region_id=REGION,
        ),
        "测试流推流记录查询",
    )
    infos = []
    if resp.body and resp.body.publish_info and resp.body.publish_info.live_stream_publish_info:
        infos = resp.body.publish_info.live_stream_publish_info
    raw: list[dict[str, Any]] = []
    for item in infos:
        if (item.stream_name or "") != name:
            continue
        pub = parse_iso(item.publish_time)
        stop = parse_iso(item.stop_time)
        if pub is None:
            continue
        live = stop is None
        seg_end = stop or datetime.now(UTC)
        if seg_end <= start or pub >= end:
            continue
        raw.append({"publish_time": pub, "stop_time": stop, "ip": item.client_addr or "", "live": live})
    return raw


def merge_online_session(raw: list[dict[str, Any]], online: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not online:
        return raw
    pub = parse_iso(online.get("publish_time"))
    ip = online.get("client_ip") or ""
    if pub is None:
        return raw
    for item in raw:
        if item.get("live") or item.get("stop_time") is None:
            if not item.get("ip") and ip:
                item["ip"] = ip
            return raw
        if item.get("publish_time") and abs((item["publish_time"] - pub).total_seconds()) < 2:
            item["stop_time"] = None
            item["live"] = True
            if ip:
                item["ip"] = ip
            return raw
    raw.append({"publish_time": pub, "stop_time": None, "ip": ip, "live": True})
    return raw


def build_test_session_details(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = sorted(raw, key=lambda x: x.get("publish_time") or datetime.min.replace(tzinfo=UTC))
    now = datetime.now(UTC)
    details: list[dict[str, Any]] = []
    prev_stop: datetime | None = None
    for index, item in enumerate(items, start=1):
        pub = item.get("publish_time")
        stop = item.get("stop_time")
        live = stop is None or item.get("live")
        end_seg = now if live else stop
        duration = (end_seg - pub).total_seconds() if pub and end_seg else None
        gap = (pub - prev_stop).total_seconds() if pub and prev_stop else None
        details.append(
            {
                "index": index,
                "start_time": fmt_cst(pub),
                "end_time": fmt_cst(stop, live=live),
                "duration": fmt_duration(duration),
                "gap": "—" if index == 1 else fmt_duration(gap),
                "ip": item.get("ip") or "—",
            }
        )
        if stop and not live:
            prev_stop = stop
    return details


def parse_cst_labeled(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip().replace(" ", "T", 1)
    if text.endswith("Z"):
        text = text[:-1] + "+08:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(UTC)


def parse_metric_points(items: list[Any], cst: bool = False) -> list[dict[str, Any]]:
    points = []
    for item in items:
        video_kbps = as_kbps(getattr(item, "video_bit_rate", None) or getattr(item, "bit_rate", None))
        audio_kbps = as_kbps(getattr(item, "audio_bit_rate", None))
        bit_kbps = as_kbps(getattr(item, "bit_rate", None))
        fps = getattr(item, "video_frame_rate", None)
        raw_time = getattr(item, "time", None)
        when = parse_cst_labeled(raw_time) if cst else parse_iso(raw_time)
        points.append(
            {
                "time": fmt_cst(when),
                "kbps": bit_kbps if bit_kbps is not None else video_kbps,
                "video_kbps": video_kbps,
                "audio_kbps": audio_kbps,
                "fps": None if fps is None else round(float(fps), 1),
            }
        )
    return [p for p in points if p["time"] != "推流中"]


def fetch_test_metrics(client: LiveClient, name: str, start: datetime, end: datetime) -> tuple[list[dict[str, Any]], str]:
    points: list[dict[str, Any]] = []
    try:
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + TEST_METRIC_CHUNK, end)
            resp = aliyun_invoke(
                client,
                "describe_live_stream_detail_frame_rate_and_bit_rate_data",
                DescribeLiveStreamDetailFrameRateAndBitRateDataRequest(
                    domain_name=TEST_PUSH_DOMAIN,
                    app_name=TEST_APP_NAME,
                    stream_name=name,
                    start_time=to_cst_z(cursor),
                    end_time=to_cst_z(chunk_end),
                    region_id=REGION,
                ),
                "码率帧率查询",
            )
            items = []
            if resp.body and resp.body.frame_rate_and_bit_rate_infos:
                items = resp.body.frame_rate_and_bit_rate_infos
            points.extend(parse_metric_points(items, cst=True))
            cursor = chunk_end
            if cursor < end:
                time.sleep(PAGE_INTERVAL_SEC)
        return points, "detail"
    except HTTPException:
        points = []
    resp = aliyun_invoke(
        client,
        "describe_live_stream_bit_rate_data",
            DescribeLiveStreamBitRateDataRequest(
                domain_name=TEST_PUSH_DOMAIN,
                app_name=TEST_APP_NAME,
                stream_name=name,
                start_time=to_utc_z(start),
                end_time=to_utc_z(end),
            ),
        "码率帧率查询",
    )
    items = []
    if resp.body and resp.body.frame_rate_and_bit_rate_infos and resp.body.frame_rate_and_bit_rate_infos.frame_rate_and_bit_rate_info:
        items = resp.body.frame_rate_and_bit_rate_infos.frame_rate_and_bit_rate_info
    return parse_metric_points(items), "bitrate"


def metric_summary(points: list[dict[str, Any]]) -> dict[str, Any]:
    def avg(key: str) -> float | None:
        values = [float(p[key]) for p in points if p.get(key) is not None]
        if not values:
            return None
        return round(sum(values) / len(values), 1)

    def extreme(key: str, fn) -> float | None:
        values = [float(p[key]) for p in points if p.get(key) is not None]
        if not values:
            return None
        return round(float(fn(values)), 1)

    return {
        "samples": len(points),
        "avg_kbps": avg("kbps"),
        "max_kbps": extreme("kbps", max),
        "min_kbps": extreme("kbps", min),
        "avg_fps": avg("fps"),
        "max_fps": extreme("fps", max),
        "min_fps": extreme("fps", min),
    }


@app.get("/api/test-streams")
def test_streams(stream: str = Query("test1")) -> dict[str, Any]:
    name = require_test_stream(stream)
    item = build_test_stream(name)
    return {
        "ok": True,
        "app": TEST_APP_NAME,
        "push_domain": TEST_PUSH_DOMAIN,
        "play_domain": TEST_PLAY_DOMAIN,
        "hours": TEST_AUTH_HOURS,
        "expire_at": item["expire_at"],
        "stream_names": TEST_STREAM_NAMES,
        "stream": item,
        "notice": "仅用于测试。请用 App Satfeeds / test1–test5，不要往正式 App sla 推流。",
    }


@app.get("/api/test-stream-status")
def test_stream_status(
    stream: str = Query("test1"),
    start: str = Query(..., description="开始时间"),
    end: str = Query(..., description="结束时间"),
) -> dict[str, Any]:
    name = require_test_stream(stream)
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)
    if not start_dt or not end_dt:
        raise HTTPException(status_code=400, detail="请提供开始时间和结束时间")
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="结束时间必须晚于开始时间")

    now = datetime.now(UTC)
    notes: list[str] = []
    if end_dt > now:
        end_dt = now
        notes.append("结束时间不能晚于当前时间，已调整到现在")
    max_span = timedelta(hours=TEST_METRIC_MAX_HOURS)
    if end_dt - start_dt > max_span:
        start_dt = end_dt - max_span
        notes.append(f"码率查询单次最多 {TEST_METRIC_MAX_HOURS} 小时，已缩短范围")
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="有效查询时间范围为空")

    client = live_client()
    state = fetch_test_state(client, name)
    time.sleep(PAGE_INTERVAL_SEC)
    online = fetch_test_online(client, name)
    time.sleep(PAGE_INTERVAL_SEC)
    sessions_raw = fetch_test_sessions(client, name, start_dt, end_dt)
    sessions_raw = merge_online_session(sessions_raw, online)
    session_details = build_test_session_details(sessions_raw)
    disconnects = max(len(session_details) - 1, 0)
    time.sleep(PAGE_INTERVAL_SEC)
    points, source = fetch_test_metrics(client, name, start_dt, end_dt)
    current = online or {
        "online": False,
        "publish_time": None,
        "client_ip": "",
        "video_kbps": None,
        "audio_kbps": None,
        "fps": None,
        "width": None,
        "height": None,
        "resolution": None,
    }
    resolution = current.get("resolution")
    for point in points:
        point["resolution"] = resolution
    latest = points[-1] if points else None
    if latest:
        if current.get("video_kbps") is None:
            current["video_kbps"] = latest.get("video_kbps")
        if current.get("audio_kbps") is None:
            current["audio_kbps"] = latest.get("audio_kbps")
        if current.get("fps") is None:
            current["fps"] = latest.get("fps")
        current["kbps"] = latest.get("kbps") or current.get("video_kbps")
    else:
        current["kbps"] = current.get("video_kbps")

    return {
        "ok": True,
        "stream_name": name,
        "app": TEST_APP_NAME,
        "push_domain": TEST_PUSH_DOMAIN,
        "start_time": fmt_cst(start_dt),
        "end_time": fmt_cst(end_dt),
        "clip_notes": notes,
        "online": state == "online" or bool(online),
        "state": "online" if (state == "online" or online) else "offline",
        "current": current,
        "sessions": session_details,
        "session_count": len(session_details),
        "disconnects": disconnects,
        "summary": metric_summary(points),
        "points": points,
        "metric_source": source,
        "resolution_note": "阿里云历史码率接口不含分辨率，分辨率取当前在线流；离线时无法读取。",
    }


@app.get("/api/streams")
def list_streams(
    start: str = Query(..., description="开始时间，ISO 或 datetime-local"),
    end: str = Query(..., description="结束时间，ISO 或 datetime-local"),
) -> dict[str, Any]:
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)
    if not start_dt or not end_dt:
        raise HTTPException(status_code=400, detail="请提供开始时间和结束时间")
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="结束时间必须晚于开始时间")

    start_dt, end_dt, clip_notes = clamp_query_window(start_dt, end_dt)

    sessions = fetch_sessions(start_dt, end_dt)
    streams = apply_match_window(attach_matches(aggregate(sessions, start_dt, end_dt)))

    def bucket(stream: dict[str, Any]) -> str | None:
        return stream.get("severity") or None

    def pack(items: list[dict[str, Any]]) -> dict[str, int]:
        return {
            "streams": len(items),
            "sessions": sum(int(s.get("sessions") or 0) for s in items),
        }

    grouped = {"critical": [], "warn": [], "ok": []}
    for stream in streams:
        key = bucket(stream)
        if key:
            grouped[key].append(stream)

    return {
        "ok": True,
        "timezone": "Asia/Shanghai",
        "keyword": KEYWORD,
        "domain": DOMAIN,
        "app": APP_NAME,
        "start_time": fmt_cst(start_dt),
        "end_time": fmt_cst(end_dt),
        "clip_notes": clip_notes,
        "limits": {
            "history_days": MAX_HISTORY_DAYS,
            "span_days": MAX_SPAN_DAYS,
            "page_size": PAGE_SIZE,
            "qps": 3,
        },
        "summary": {
            "all": pack(streams),
            "billable": pack(grouped["ok"] + grouped["warn"]),
            "critical": pack(grouped["critical"]),
            "warn": pack(grouped["warn"]),
            "ok": pack(grouped["ok"]),
        },
        "streams": streams,
    }
