"""
인기검색어 / 인기상품 통합 보드 — 로컬 서버.

실행:  python server.py          →  http://127.0.0.1:8787

구조
----
브라우저 ── 30초마다 ──▶ 이 서버의 캐시(/api/snapshot)     : 공짜, 부담 없음
이 서버 ── 소스별 주기 ──▶ 실제 사이트                      : 5분 ~ 6시간

브라우저에서 직접 네이버·G마켓을 fetch 하는 방식은 CORS 때문에 애초에 막힌다.
그래서 수집은 여기서 하고, 화면은 이 서버만 바라본다.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from collectors import (
    BUILD,
    DEFAULT_PERIOD,
    PERIODS,
    Collector,
    Item,
    SourceResult,
    build_collectors,
    now_kst,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("board")
# 요청 한 줄 한 줄까지 찍히면 정작 필요한 수집 결과가 묻힌다
logging.getLogger("httpx").setLevel(logging.WARNING)

BASE_DIR = Path(__file__).parent
COLLECTORS: list[Collector] = build_collectors()

# key -> 최신 결과
CACHE: dict[str, SourceResult] = {}
# key -> 다음 수집 예정 시각
NEXT_DUE: dict[str, datetime] = {}
# 지금 수집 중인 소스
RUNNING: set[str] = set()

# 서버를 껐다 켜도 목록과 순위 변동 기준이 남도록 디스크에 둔다.
STATE_FILE = BASE_DIR / "state.json"


DEMO: list[bool] = []
STATE_FILE = BASE_DIR / "state.json"


def save_state() -> None:
    """서버를 껐다 켜도 목록과 순위 변동 기준이 남도록 파일에 적어 둔다."""
    if DEMO:
        return
    try:
        payload = {
            "cache": {
                k: {
                    "items": [i.to_dict() for i in v.items],
                    "ok": v.ok,
                    "error": v.error,
                    "warning": v.warning,
                    "blocked": v.blocked,
                    "fetchedAt": v.fetched_at.isoformat() if v.fetched_at else None,
                }
                for k, v in CACHE.items()
                if v.items
            },
            "nextDue": {k: v.isoformat() for k, v in NEXT_DUE.items()},
            "build": BUILD,
        }
        STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        log.warning("상태 저장 실패: %s", exc)


def load_state() -> None:
    if not STATE_FILE.exists():
        return
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning("저장된 상태를 읽지 못했습니다: %s", exc)
        return

    by_key = {c.key: c for c in COLLECTORS}
    for key, saved in (payload.get("cache") or {}).items():
        c = by_key.get(key)
        if not c:
            continue
        result = _placeholder(c)
        result.items = [
            Item(
                rank=d.get("rank", n),
                title=d.get("title", ""),
                url=d.get("url"),
                price=d.get("price"),
                image=d.get("image"),
                group=d.get("group"),
                meta=d.get("meta"),
            )
            for n, d in enumerate(saved.get("items", []), start=1)
        ]
        result.ok = saved.get("ok", True)
        result.error = saved.get("error")
        result.blocked = saved.get("blocked", False)
        result.warning = saved.get("warning")
        if saved.get("fetchedAt"):
            with contextlib.suppress(ValueError):
                result.fetched_at = datetime.fromisoformat(saved["fetchedAt"])
        CACHE[key] = result

    # 고른 기간은 일부러 복원하지 않는다. 켤 때마다 일간으로 시작한다.

    # 코드를 고쳤는데 "다음 수집은 6시간 뒤"가 남아 있으면 수정이 반영되지 않는다.
    # 빌드가 달라졌으면 일정을 버리고 지금 바로 다시 받는다.
    saved_build = payload.get("build")
    if saved_build != BUILD:
        log.info("빌드가 %s에서 %s로 바뀌어 수집 일정을 초기화합니다.", saved_build or "(없음)", BUILD)
    else:
        for key, iso in (payload.get("nextDue") or {}).items():
            with contextlib.suppress(ValueError):
                NEXT_DUE[key] = datetime.fromisoformat(iso)

    log.info("저장된 상태를 불러왔습니다 (%d개 소스)", len(payload.get("cache") or {}))


def _placeholder(c: Collector) -> SourceResult:
    return SourceResult(
        key=c.key,
        label=c.label,
        kind=c.kind,
        source_url=c.source_url,
        ok=False,
        error=None if c.enabled else "설정 필요",
        note=c.note,
        interval=c.interval,
    )


async def refresh_one(collector: Collector, client: httpx.AsyncClient) -> None:
    if DEMO:
        if collector.key in CACHE and CACHE[collector.key].items:
            CACHE[collector.key].fetched_at = now_kst()
        return
    if not collector.enabled:
        CACHE[collector.key] = _placeholder(collector)
        return

    log.info("수집 시작 %s", collector.key)
    RUNNING.add(collector.key)
    try:
        result = await collector.run(client)
    finally:
        RUNNING.discard(collector.key)

    if result.ok and result.items:
        CACHE[collector.key] = result
        log.info("수집 완료 %s — %d건", collector.key, len(result.items))
        if result.warning:
            log.warning("%s — %s", collector.key, result.warning)
    else:
        # 실패했다고 기존 데이터를 버리지 않는다. 오래된 값이라도 보여주는 편이 낫다.
        old = CACHE.get(collector.key)
        if old and old.items:
            old.error = result.error
            old.warning = result.warning
            old.ok = False
            old.blocked = result.blocked
            CACHE[collector.key] = old
        else:
            CACHE[collector.key] = result
        log.warning("수집 실패 %s — %s", collector.key, result.error)

    if result.blocked:
        log.warning("%s 자동 수집이 차단되었습니다. 자동 재시도를 멈춥니다.", collector.key)
        NEXT_DUE[collector.key] = now_kst() + timedelta(days=365)
    else:
        NEXT_DUE[collector.key] = now_kst() + timedelta(seconds=collector.interval)
    save_state()


async def scheduler() -> None:
    limits = httpx.Limits(max_connections=8)
    async with httpx.AsyncClient(
        timeout=20.0, follow_redirects=True, limits=limits
    ) as client:
        while True:
            now = now_kst()
            due = [
                c
                for c in COLLECTORS
                if c.enabled and NEXT_DUE.get(c.key, now) <= now
            ]
            if due:
                await asyncio.gather(
                    *(refresh_one(c, client) for c in due), return_exceptions=True
                )
                save_state()
            await asyncio.sleep(5)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    report_dashboard()
    for c in COLLECTORS:
        CACHE[c.key] = _placeholder(c)
    if DEMO:
        load_demo()
        yield
        return
    load_state()
    task = asyncio.create_task(scheduler())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="쇼핑 트렌드", lifespan=lifespan)


DASHBOARD = BASE_DIR / "dashboard.html"


def dashboard_build() -> str | None:
    """화면 파일에 박힌 빌드 번호를 읽는다. 어느 파일을 쓰고 있는지 확인하기 위한 것."""
    try:
        text = DASHBOARD.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    m = re.search(r'DASHBOARD_BUILD\s*=\s*"([^"]+)"', text)
    return m.group(1) if m else None


def report_dashboard() -> None:
    if not DASHBOARD.exists():
        log.error("화면 파일이 없습니다: %s", DASHBOARD)
        return
    size = DASHBOARD.stat().st_size
    found = dashboard_build()
    log.info("화면 파일  %s (%s바이트, 빌드 %s)", DASHBOARD, f"{size:,}", found or "표시 없음")
    if found != BUILD:
        log.error("이 파일은 서버(%s)와 버전이 다릅니다. 위 경로의 파일을 바꿔야 합니다.", BUILD)


NO_CACHE = {
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}


@app.get("/")
async def index(v: str | None = None):
    """
    주소에 빌드 번호를 붙여 다시 보낸다.
    캐시 차단 헤더는 이미 저장된 페이지에는 듣지 않지만, 주소가 달라지면 확실히 새로 받는다.
    """
    if v != BUILD:
        return RedirectResponse(f"/?v={BUILD}", status_code=307, headers=NO_CACHE)
    return FileResponse(DASHBOARD, headers=NO_CACHE)


def build_snapshot() -> dict:
    payload = []
    for c in COLLECTORS:
        result = CACHE.get(c.key) or _placeholder(c)
        data = result.to_dict()
        data["nextDue"] = (
            NEXT_DUE[c.key].isoformat() if c.key in NEXT_DUE else None
        )
        data["enabled"] = c.enabled
        data["running"] = c.key in RUNNING
        data["period"] = getattr(c, "period", None)
        data["periods"] = PERIODS if getattr(c, "period", None) else None
        payload.append(data)
    return {
        "serverTime": now_kst().isoformat(),
        "build": BUILD,
        "dashboardPath": str(DASHBOARD),
        "dashboardBuild": dashboard_build(),
        "sources": payload,
    }


@app.get("/api/snapshot")
async def snapshot() -> JSONResponse:
    return JSONResponse(build_snapshot())


async def refresh_in_background(collector: Collector) -> None:
    """화면을 붙잡아 두지 않고 뒤에서 수집한다. 진행 상황은 수집 중 표시로 보인다."""
    try:
        async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
            await refresh_one(collector, client)
    finally:
        RUNNING.discard(collector.key)
    save_state()


@app.post("/api/period/{key}")
async def set_period(key: str, body: dict, tasks: BackgroundTasks) -> JSONResponse:
    """화면에서 일간/주간/월간을 바꾸면 곧바로 다시 수집한다."""
    target = next((c for c in COLLECTORS if c.key == key), None)
    if target is None or getattr(target, "period", None) is None:
        return JSONResponse({"error": "기간을 바꿀 수 없는 소스입니다."}, status_code=404)

    period = str(body.get("period", ""))
    if period not in PERIODS:
        return JSONResponse({"error": f"모르는 기간입니다: {period}"}, status_code=400)

    target.period = period
    NEXT_DUE.pop(key, None)
    # 응답을 기다리는 사이에도 화면이 수집 중으로 보이도록 먼저 표시해 둔다
    RUNNING.add(key)
    log.info("%s 기간을 %s 로 바꿉니다.", key, PERIODS[period])
    tasks.add_task(refresh_in_background, target)
    return JSONResponse({"started": True, "period": period})


def load_demo() -> None:
    """--demo 로 띄우면 네트워크 없이 화면만 확인할 수 있다."""
    cats = __import__("collectors").NAVER_CIDS
    samples = {
        "naver_datalab_keyword": [
            (name, [f"{name} 검색어 {i}" for i in range(1, 11)]) for _cid, name in cats
        ],
        "naver_section": [
            (name, ["아우터", "원피스", "블라우스/셔츠", "티셔츠", "티셔츠",
                    "니트", "아우터", "바지", "스커트", "바지"])
            for _cid, name in cats
        ],
        "snx_best": [(None, [f"네이버쇼핑 인기상품 {i}" for i in range(1, 101)])],
        "elevenst_best": [(None, [f"11번가 BEST {i}" for i in range(1, 101)])],
        "auction_best": [(None, [f"옥션 BEST {i}" for i in range(1, 101)])],
    }
    for c in COLLECTORS:
        rows = samples.get(c.key)
        if not rows:
            CACHE[c.key] = _placeholder(c)
            continue
        items, n = [], 0
        for group, titles in rows:
            for i, t in enumerate(titles, start=1):
                n += 1
                items.append(
                    Item(rank=i, title=t, url=c.source_url, group=group,
                         price=(n * 7300 if c.kind == "product" else None))
                )
        CACHE[c.key] = SourceResult(
            key=c.key, label=c.label, kind=c.kind, source_url=c.source_url,
            items=items, ok=True, note=c.note, fetched_at=now_kst(), interval=c.interval,
        )
        NEXT_DUE[c.key] = now_kst() + timedelta(days=365)


CURL_SKIP_FLAGS = {
    "--compressed", "-s", "--silent", "-i", "-k", "--insecure",
    "-L", "--location", "-v", "--verbose", "-g", "--globoff",
}


def parse_curl(text: str) -> dict:
    """
    크롬 개발자도구의 'Copy as cURL (bash)' 결과를 요청 정보로 바꾼다.
    주소, 방식, 헤더, 본문만 뽑고 나머지 옵션은 무시한다.
    """
    import shlex

    text = text.replace("\\\n", " ").replace("^\n", " ").strip()
    if not text:
        raise ValueError("파일이 비어 있습니다.")
    parts = shlex.split(text)

    url = None
    method = None
    headers: dict[str, str] = {}
    body = None

    i = 0
    while i < len(parts):
        token = parts[i]
        if token == "curl" or token in CURL_SKIP_FLAGS:
            pass
        elif token in ("-X", "--request"):
            method = parts[i + 1]
            i += 1
        elif token in ("-H", "--header"):
            k, _, v = parts[i + 1].partition(":")
            if k.strip().lower() not in {"content-length", "host"}:
                headers[k.strip()] = v.strip()
            i += 1
        elif token in ("-b", "--cookie"):
            headers["Cookie"] = parts[i + 1]
            i += 1
        elif token in ("-d", "--data", "--data-raw", "--data-binary", "--data-urlencode"):
            body = parts[i + 1]
            i += 1
        elif token.startswith("http"):
            url = token
        i += 1

    if not url:
        raise ValueError("cURL 안에서 주소를 찾지 못했습니다. 'Copy as cURL (bash)' 로 복사했는지 확인해 주세요.")
    return {"url": url, "method": method or ("POST" if body else "GET"), "headers": headers, "body": body}


async def from_curl(path: str) -> None:
    """cURL 을 그대로 보내보고, 응답에서 상품 목록을 찾아 붙여넣을 설정을 만들어 준다."""
    from collectors import find_object_lists

    text = Path(path).read_text(encoding="utf-8", errors="replace")
    try:
        req = parse_curl(text)
    except ValueError as exc:
        print(f"\n{exc}\n")
        return

    print(f"\n주소   {req['url'][:120]}")
    print(f"방식   {req['method']}, 헤더 {len(req['headers'])}개, 본문 {'있음' if req['body'] else '없음'}\n")

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.request(
            req["method"], req["url"], headers=req["headers"], content=req["body"]
        )
    print(f"응답   HTTP {resp.status_code}, {len(resp.content):,}바이트")

    try:
        data = resp.json()
    except Exception:  # noqa: BLE001
        print("\n이 응답은 JSON이 아닙니다. 목록을 담고 있는 다른 요청을 찾아보세요.\n")
        print(resp.text[:400])
        return

    found = sorted(find_object_lists(data), key=lambda c: -c[1])
    if not found:
        print("\n객체 배열이 없습니다. 목록을 담은 다른 요청일 수 있습니다.\n")
        return

    print("\n찾은 배열\n")
    for path_, length, keys in found[:6]:
        print(f"  {length:4}개  {path_ or '(최상위)'}")
        print(f"         키    {', '.join(keys)}")

    best_path, best_len, best_keys = found[0]
    from collectors import ID_KEYS, PRICE_KEYS, TITLE_KEYS

    def pick(cands):
        low = {k.lower(): k for k in best_keys}
        for c in cands:
            if c.lower() in low:
                return low[c.lower()]
        return None

    title = pick(TITLE_KEYS) or "(직접 확인)"
    ident = pick(ID_KEYS)
    price = pick(PRICE_KEYS)

    print(f"\n{'-' * 62}")
    print("collectors.py 의 build_collectors() 안에 아래를 넣으세요.\n")
    print("        JsonApiCollector(")
    print('            key="여기에_소스_이름",')
    print('            label="화면에 보일 이름",')
    print('            source_url="사이트 주소",')
    print(f'            api_url="{req["url"]}",')
    if req["method"] != "GET":
        print(f'            method="{req["method"]}",')
    if req["body"]:
        print(f'            data={req["body"]!r},')
    print(f'            list_path="{best_path}",')
    print(f'            title_key="{title}",')
    if ident:
        print(f'            id_key="{ident}",')
        print('            url_template="상세 주소 형식/{id}",')
    if price:
        print(f'            price_key="{price}",')
    print("            interval=600,")
    print("            enabled=True,")
    print("        ),")
    print(f"{'-' * 62}\n")
    print(f"({best_len}개짜리 배열을 골랐습니다. 다른 배열이 맞으면 list_path 를 바꾸세요.)\n")


async def export_snapshot(path: str, every_sec: int = 1200) -> None:
    """
    켜져 있는 소스를 수집해 결과를 JSON 파일로 저장한다.
    깃허브 액션처럼 서버를 띄울 수 없는 곳에서 화면에 쓸 자료를 만들기 위한 것.

    기간을 고를 수 있는 소스는 일간/주간/월간을 모두 받아 함께 담는다.
    서버가 없어도 화면에서 버튼만 눌러 바꿔 볼 수 있게 하기 위해서다.

    every_sec 은 이 파일을 다시 만드는 주기다. 화면이 다음 예정 시각을 셀 때 쓴다.
    """
    for c in COLLECTORS:
        CACHE[c.key] = _placeholder(c)
    load_state()

    targets = [c for c in COLLECTORS if c.enabled]
    period_sources = [c for c in targets if getattr(c, "period", None)]
    plain_sources = [c for c in targets if not getattr(c, "period", None)]

    limits = httpx.Limits(max_connections=8)
    per_period: dict[str, dict[str, dict]] = {}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, limits=limits) as client:
        if plain_sources:
            await asyncio.gather(
                *(refresh_one(c, client) for c in plain_sources), return_exceptions=True
            )

        # 기본 기간을 마지막에 돌린다. 화면이 처음 열릴 때 그 기간이 보여야 한다.
        order = [p for p in PERIODS if p != DEFAULT_PERIOD] + [DEFAULT_PERIOD]
        for period in order:
            if not period_sources:
                break
            for c in period_sources:
                c.period = period
                NEXT_DUE.pop(c.key, None)
            print(f"\n[{PERIODS[period]}] 수집")
            await asyncio.gather(
                *(refresh_one(c, client) for c in period_sources), return_exceptions=True
            )
            for c in period_sources:
                r = CACHE[c.key]
                per_period.setdefault(c.key, {})[period] = {
                    "items": [i.to_dict() for i in r.items],
                    "note": r.note,
                    "warning": r.warning,
                    "ok": r.ok,
                    "error": r.error,
                }

    data = build_snapshot()
    data["static"] = True          # 화면이 정적 배포임을 알아채는 표시
    data["everySec"] = every_sec   # 다음 예정 시각을 셀 때 쓴다
    data.pop("dashboardPath", None)
    for src in data["sources"]:
        if src["key"] in per_period:
            src["periodItems"] = per_period[src["key"]]

    out = Path(path)
    out.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    ok = sum(1 for c in targets if CACHE[c.key].ok)
    total = sum(len(CACHE[c.key].items) for c in targets)
    print(f"\n{out} 저장 완료. {ok}/{len(targets)}개 소스, {total}건, {out.stat().st_size:,}바이트\n")
    for c in targets:
        r = CACHE[c.key]
        state = f"{len(r.items)}건" if r.ok else f"실패 ({r.error})"
        if c.key in per_period:
            counts = ", ".join(
                f"{PERIODS[p]} {len(v['items'])}건" for p, v in per_period[c.key].items()
            )
            state = counts
        print(f"  {c.label:28} {state}")
    print()
    if ok == 0:
        raise SystemExit(1)


async def preview_source(key: str) -> None:
    """
    실제 수집기를 한 번 돌려 결과를 그대로 보여준다.
    사이트 화면의 순위와 나란히 놓고 비교하기 위한 것.
    """
    target = next((c for c in COLLECTORS if c.key == key), None)
    if target is None:
        print(f"\n'{key}' 소스를 찾을 수 없습니다. 가능한 값: {', '.join(c.key for c in COLLECTORS)}\n")
        return

    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        result = await target.run(client)

    print(f"\n{target.label}")
    print(f"{target.source_url}\n")

    shape = getattr(target, "price_api_shape", None)
    if shape:
        print(f"가격 주소  {shape} 형식으로 받았습니다\n")

    paths = getattr(target, "picked_paths", None)
    if paths:
        print("고른 배열")
        for path in paths:
            print(f"  {path}")
        print()

    if not result.ok and not result.items:
        print(f"실패: {result.error}\n")
        return
    if result.warning:
        print(f"경고: {result.warning}\n")

    print(f"{'순위':<5}{'상품명':<52}{'가격':>12}")
    print("-" * 70)
    for item in result.items:
        title = item.title if len(item.title) <= 50 else item.title[:49] + "…"
        price = f"{item.price:,}원" if item.price else "-"
        print(f"{item.rank:<5}{title:<52}{price:>12}")
        if target.kind == "product":
            print(f"     링크   {item.url or '(없음)'}")
            print(f"     이미지 {item.image or '(못 찾음)'}")
    print(f"\n총 {len(result.items)}건")
    if result.items:
        print(f"1위 링크  {result.items[0].url}\n")


async def inspect_source(key: str) -> None:
    """
    페이지는 열리는데 항목이 안 뽑힐 때 쓴다.
    실제로 어떤 형태의 링크가 들어 있는지 세어서 보여주므로,
    어떤 패턴을 잡아야 하는지 짐작하지 않고 바로 알 수 있다.
    """
    import re as _re
    from collections import Counter
    from urllib.parse import urlparse, parse_qs

    from bs4 import BeautifulSoup
    from collectors import (
        BROWSER_HEADERS,
        decode_html,
        extract_embedded_json,
        find_object_lists,
        looks_like_bot_wall,
    )

    target = next((c for c in COLLECTORS if c.key == key), None)
    if target is None:
        print(f"'{key}' 소스를 찾을 수 없습니다. 가능한 값: {', '.join(c.key for c in COLLECTORS)}")
        return

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        warm = getattr(target, "warmup_url", None)
        if warm:
            with contextlib.suppress(Exception):
                await client.get(warm, headers=BROWSER_HEADERS)
        resp = await client.get(target.source_url, headers=BROWSER_HEADERS)

    html = decode_html(resp, getattr(target, "charset", None))

    print(f"\n주소      {target.source_url}")
    print(f"응답      HTTP {resp.status_code}, {len(html):,}자")
    if looks_like_bot_wall(html):
        print("판정      봇 확인 화면입니다. 선택자 문제가 아닙니다.\n")
        return

    # 1) HTML 안에 심긴 데이터 덩어리부터 본다. 여기 있으면 XHR 주소를 몰라도 된다.
    blobs = extract_embedded_json(html)
    if blobs:
        candidates: list[tuple[str, int, list[str]]] = []
        for blob in blobs:
            candidates += find_object_lists(blob)
        candidates.sort(key=lambda c: -c[1])
        print(f"심긴 데이터  덩어리 {len(blobs)}개, 객체 배열 {len(candidates)}개\n")
        for path, length, keys in candidates[:8]:
            print(f"  {length:4}개  {path or '(최상위)'}")
            print(f"         키    {', '.join(keys)}")
        print()
        if candidates:
            print("이 중 상품 목록으로 보이는 줄의 경로를 list_path 에 넣으면 됩니다.")
            print("자동으로 못 고르면 그 경로를 알려 주세요.\n")
    else:
        print("심긴 데이터  없음\n")

    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("a[href]")
    print(f"링크      {len(anchors)}개\n")

    # 경로 + 쿼리 키 조합으로 묶으면 상품 링크가 덩어리로 드러난다
    shapes: Counter = Counter()
    samples: dict[str, tuple[str, str]] = {}
    for a in anchors:
        href = a.get("href", "")
        if not href or href.startswith(("#", "javascript:")):
            continue
        u = urlparse(href)
        path = _re.sub(r"/\d{3,}", "/{숫자}", u.path)
        keys = ",".join(sorted(parse_qs(u.query).keys()))
        shape = f"{u.netloc or '(같은 도메인)'}{path}" + (f"?{keys}" if keys else "")
        shapes[shape] += 1
        if shape not in samples:
            text = " ".join((a.get("title") or a.get_text(" ", strip=True) or "").split())[:40]
            samples[shape] = (href[:90], text)

    print("가장 많이 나온 링크 형태 (상품 목록이면 개수가 수십 개입니다)\n")
    for shape, count in shapes.most_common(15):
        href, text = samples[shape]
        print(f"  {count:4}회  {shape}")
        print(f"         예시  {href}")
        if text:
            print(f"         글자  {text}")
    print()
    if max(shapes.values(), default=0) < 10:
        print("상품 링크로 보이는 덩어리가 없습니다. 목록을 자바스크립트로 그리는 구조일 수 있습니다.")
        print("F12 → Network → Fetch/XHR 에서 순위를 실어오는 요청을 찾아보세요.\n")


async def dump_html(key: str) -> None:
    """선택자가 안 맞을 때 실제로 뭘 받았는지 확인용. 받은 HTML을 파일로 떨군다."""
    target = next((c for c in COLLECTORS if c.key == key), None)
    if target is None:
        print(f"'{key}' 소스를 찾을 수 없습니다.")
        return
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(
            target.source_url,
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9"},
        )
    out = BASE_DIR / f"dump_{key}.html"
    out.write_text(resp.text, encoding="utf-8")
    print(f"HTTP {resp.status_code} · {len(resp.text):,}자 → {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--dump", help="해당 소스의 원본 HTML을 파일로 저장하고 종료")
    parser.add_argument("--inspect", help="해당 소스 페이지에 어떤 링크가 들어 있는지 분석하고 종료")
    parser.add_argument("--preview", help="해당 소스를 한 번 수집해 결과 목록을 그대로 출력하고 종료")
    parser.add_argument("--export", metavar="파일",
                        help="한 번 수집해 JSON 으로 저장하고 종료 (정적 배포용)")
    parser.add_argument("--every", type=int, default=1200, metavar="초",
                        help="정적 배포에서 이 파일을 다시 만드는 주기 (기본 1200초)")
    parser.add_argument("--from-curl", dest="from_curl", metavar="파일",
                        help="개발자도구에서 복사한 cURL 을 읽어 수집기 설정을 만들어 줍니다")
    parser.add_argument("--demo", action="store_true", help="네트워크 없이 예시 데이터로 화면만 확인")
    parser.add_argument("--reset", action="store_true", help="저장된 목록을 지우고 처음부터 다시 수집")
    args = parser.parse_args()

    if args.reset:
        STATE_FILE.unlink(missing_ok=True)
        print("저장된 목록을 지웠습니다.")

    if args.export:
        asyncio.run(export_snapshot(args.export, args.every))
    elif args.preview:
        asyncio.run(preview_source(args.preview))
    elif args.from_curl:
        asyncio.run(from_curl(args.from_curl))
    elif args.inspect:
        asyncio.run(inspect_source(args.inspect))
    elif args.dump:
        asyncio.run(dump_html(args.dump))
    else:
        import uvicorn

        if args.demo:
            DEMO.append(True)
        print(f"\n  쇼핑 트렌드  빌드 {BUILD}")
        print(f"  보드 주소  http://{args.host}:{args.port}\n")
        # uvicorn 은 포트 오류를 자기가 삼켜서 조용히 끝난다. 미리 확인해 알려준다.
        import socket

        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind((args.host, args.port))
            except OSError as exc:
                print(f"  [문제] {args.port}번 포트를 이미 다른 프로그램이 쓰고 있습니다.")
                print("  보드가 다른 창에서 돌고 있지 않은지 확인해 주세요.")
                print(f"  다른 번호로 띄우려면:  python server.py --port 8788")
                print(f"  ({exc})\n")
                raise SystemExit(1) from exc

        # log_config=None: uvicorn 이 로깅을 다시 잡으면 같은 줄이 두 번 찍힌다
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning", log_config=None)
