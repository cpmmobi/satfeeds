from __future__ import annotations

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
from alibabacloud_live20161101.models import DescribeLiveStreamsPublishListRequest
from alibabacloud_tea_openapi.models import Config
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

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
