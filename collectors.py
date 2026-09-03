"""
사이트별 인기검색어 / 인기상품 수집기.

설계 원칙
---------
1) 사이트마다 갱신 주기가 다르다. 네이버 데이터랩 쇼핑인사이트는 하루 단위로 집계되고,
   G마켓/옥션 BEST는 보통 수십 분 단위로 바뀐다. 그래서 수집기마다 interval(초)을 따로 둔다.
   화면 자동 갱신(30초)은 "로컬 서버의 캐시"를 다시 그리는 것이고,
   외부 사이트를 실제로 다시 긁는 주기는 여기 interval이 결정한다.
2) 엔드포인트가 바뀌어도 코드를 갈아엎지 않도록, 범용 수집기 두 개
   (JsonApiCollector / LinkHarvestCollector)를 두고 설정만 바꿔 쓰게 했다.
3) 실패는 화면에 그대로 노출한다. 조용히 빈 목록을 반환하지 않는다.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import httpx
from bs4 import BeautifulSoup

# 파일이 섞였는지 눈으로 확인하기 위한 표시. 세 파일의 값이 같아야 한다.
BUILD = "2026-09-01.76"

KST = timezone(timedelta(hours=9))

# 소스마다 받아올 최대 개수. 소스별로 다르게 하려면 각 수집기의 limit 을 따로 주면 된다.
DEFAULT_LIMIT = 30
# 쇼핑몰 베스트처럼 목록이 긴 소스에 쓰는 값
PRODUCT_LIMIT = 100

# 데이터랩이 쓰는 집계 단위. 화면에서 고를 수 있게 소스에 붙인다.
PERIODS = {"date": "일간", "week": "주간", "month": "월간"}
# 처음 열었을 때 선택되어 있을 기간
DEFAULT_PERIOD = "week"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# 평범한 브라우저 방문처럼 보이게 하는 기본 헤더.
# 헤더가 부실하면 봇으로 보고 빈 페이지나 차단 페이지를 주는 곳이 있다.
BROWSER_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def now_kst() -> datetime:
    return datetime.now(KST)


class BotBlocked(Exception):
    """사이트가 자동 수집을 감지해 차단 화면을 돌려준 경우."""


# 차단 화면에 흔히 들어가는 문구. 상품 목록 페이지에는 나올 일이 없는 표현들이다.
BOT_WALL_MARKERS = (
    "봇 확인",
    "봇(Bot)",
    "검토번호",
    "자동입력 방지",
    "비정상적인 접근",
    "Access Denied",
    "unusual traffic",
    "Are you a robot",
)


def decode_html(resp: httpx.Response, charset: str | None) -> str:
    """
    지정한 인코딩으로 먼저 읽되, 깨진 글자가 많으면 응답이 말하는 인코딩으로 되돌린다.
    사이트가 EUC-KR에서 UTF-8로 넘어가도 한글이 깨지지 않는다.
    """
    if not charset:
        return resp.text
    text = resp.content.decode(charset, errors="replace")
    broken = text.count("\ufffd")
    if text and broken / len(text) > 0.01:
        return resp.text
    return text


def looks_like_bot_wall(html: str) -> bool:
    # 차단 화면은 대체로 짧다. 긴 페이지에 저 단어가 우연히 섞인 경우와 구분한다.
    if len(html) > 200_000:
        return False
    return any(m in html for m in BOT_WALL_MARKERS)


# ---------------------------------------------------------------- 자료구조


@dataclass
class Item:
    rank: int
    title: str
    url: str | None = None
    price: int | None = None
    image: str | None = None
    group: str | None = None      # 카테고리 탭이 있는 소스용
    meta: str | None = None       # 화면에 작게 붙일 부가 정보

    def to_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "title": self.title,
            "url": self.url,
            "price": self.price,
            "image": self.image,
            "group": self.group,
            "meta": self.meta,
        }


@dataclass
class SourceResult:
    key: str
    label: str
    kind: str                     # "keyword" | "product"
    source_url: str
    items: list[Item] = field(default_factory=list)
    ok: bool = True
    error: str | None = None
    note: str | None = None
    warning: str | None = None
    period: str | None = None
    blocked: bool = False
    fetched_at: datetime | None = None
    interval: int = 600

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "kind": self.kind,
            "sourceUrl": self.source_url,
            "items": [i.to_dict() for i in self.items],
            "ok": self.ok,
            "error": self.error,
            "note": self.note,
            "warning": self.warning,
            "period": self.period,
            "blocked": self.blocked,
            "fetchedAt": self.fetched_at.isoformat() if self.fetched_at else None,
            "interval": self.interval,
        }


class Collector:
    key: str = "base"
    label: str = "이름 없음"
    kind: str = "product"
    source_url: str = ""
    interval: int = 600           # 외부 사이트 재수집 주기(초)
    enabled: bool = True
    note: str | None = None
    warning: str | None = None
    limit: int = DEFAULT_LIMIT
    period: str | None = None      # 기간 선택을 지원하는 소스만 값을 가진다

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        raise NotImplementedError

    async def run(self, client: httpx.AsyncClient) -> SourceResult:
        result = SourceResult(
            key=self.key,
            label=self.label,
            kind=self.kind,
            source_url=self.source_url,
            note=self.note,
            period=self.period,
            interval=self.interval,
            fetched_at=now_kst(),
        )
        self.warning = None
        try:
            result.items = await self.fetch(client)
            result.warning = self.warning
            result.note = self.note
            result.period = self.period
            if not result.items:
                result.ok = False
                result.error = "응답은 받았지만 항목을 하나도 뽑지 못했습니다. 선택자나 엔드포인트가 바뀐 것 같습니다."
        except BotBlocked as exc:
            result.ok = False
            result.blocked = True
            result.error = str(exc)
        except httpx.HTTPStatusError as exc:
            result.ok = False
            result.error = f"HTTP {exc.response.status_code} 응답"
        except httpx.RequestError as exc:
            result.ok = False
            result.error = f"연결 실패: {exc.__class__.__name__}"
        except Exception as exc:  # noqa: BLE001
            result.ok = False
            result.error = f"{exc.__class__.__name__}: {exc}"
        return result


# ---------------------------------------------------------------- 유틸


def dig(data: Any, path: str) -> Any:
    """'data.items[0].list' 같은 점 표기 경로로 중첩 JSON을 파고든다."""
    if not path:
        return data
    cur = data
    for token in path.split("."):
        while token.endswith("]"):
            token, _, idx = token[:-1].rpartition("[")
            if token:
                cur = cur[token]
            cur = cur[int(idx)]
            token = ""
        if token:
            cur = cur[token]
    return cur


PRICE_RE = re.compile(r"([0-9][0-9,]{2,})\s*원")


# 가격 전용 요소 안에서는 '원' 없이 숫자만 있는 경우도 가격으로 본다
BARE_NUMBER_RE = re.compile(r"^[^0-9]{0,4}([0-9]{1,3}(?:,[0-9]{3})+)[^0-9]{0,6}$")


def parse_bare_number(text: str | None) -> int | None:
    m = BARE_NUMBER_RE.match((text or "").strip())
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_price(text: str | None) -> int | None:
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def naver_shopping_search(keyword: str) -> str:
    q = urllib.parse.quote(keyword)
    return f"https://search.shopping.naver.com/search/all?query={q}"


# ---------------------------------------------------------------- 범용 수집기


class JsonApiCollector(Collector):
    """
    DevTools(F12) → Network → Fetch/XHR 에서 찾은 JSON 엔드포인트를 그대로 꽂아 쓰는 수집기.

    사이트가 SPA(자바스크립트로 목록을 그리는 구조)면 HTML을 긁어봐야 빈 껍데기만 나온다.
    그럴 때 목록을 실어오는 XHR 요청을 찾아 아래 값만 채우면 된다.
    """

    def __init__(
        self,
        *,
        key: str,
        label: str,
        source_url: str,
        kind: str = "product",
        api_url: str | None = None,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json_body: dict | None = None,
        headers: dict | None = None,
        list_path: str = "",
        title_key: str = "title",
        url_key: str | None = None,
        url_template: str | None = None,     # "{id}" 자리에 id_key 값이 들어간다
        id_key: str | None = None,
        price_key: str | None = None,
        image_key: str | None = None,
        interval: int = 600,
        limit: int = DEFAULT_LIMIT,
        enabled: bool = True,
        note: str | None = None,
    ):
        self.key, self.label, self.kind = key, label, kind
        self.source_url = source_url
        self.api_url = api_url
        self.method = method.upper()
        self.params, self.data, self.json_body = params, data, json_body
        self.headers = headers or {}
        self.list_path = list_path
        self.title_key, self.url_key, self.url_template = title_key, url_key, url_template
        self.id_key, self.price_key, self.image_key = id_key, price_key, image_key
        self.interval, self.limit, self.enabled, self.note = interval, limit, enabled, note

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        if not self.api_url:
            raise RuntimeError(
                "API 주소가 비어 있습니다. README의 '엔드포인트 찾는 법'을 보고 api_url을 채워주세요."
            )
        headers = {
            **BROWSER_HEADERS,
            "Accept": "application/json, text/plain, */*",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Referer": self.source_url,
            **self.headers,
        }
        resp = await client.request(
            self.method,
            self.api_url,
            params=self.params,
            data=self.data,
            json=self.json_body,
            headers=headers,
        )
        resp.raise_for_status()
        rows = dig(resp.json(), self.list_path)
        items: list[Item] = []
        for i, row in enumerate(rows[: self.limit], start=1):
            title = str(dig(row, self.title_key))
            url = None
            if self.url_key:
                url = dig(row, self.url_key)
            elif self.url_template and self.id_key:
                url = self.url_template.format(id=dig(row, self.id_key))
            elif self.kind == "keyword":
                url = naver_shopping_search(title)
            price = None
            if self.price_key:
                try:
                    price = int(str(dig(row, self.price_key)).replace(",", ""))
                except Exception:  # noqa: BLE001
                    price = None
            image = dig(row, self.image_key) if self.image_key else None
            items.append(Item(rank=i, title=title, url=url, price=price, image=image))
        return items


def to_int_price(value: Any) -> int | None:
    """'56,970' 도 56970.0000 도 정수로 바꾼다."""
    try:
        return int(round(float(str(value).replace(",", "").strip())))
    except (TypeError, ValueError):
        return None


def unwrap_asmx(data: Any) -> Any:
    """ASP.NET 웹서비스는 결과를 d 안에 문자열로 넣어 보내는 일이 있다."""
    if isinstance(data, dict) and "d" in data and len(data) == 1:
        inner = data["d"]
        if isinstance(inner, str):
            with contextlib.suppress(json.JSONDecodeError, ValueError):
                return json.loads(inner)
        return inner
    return data


def find_price_map(data: Any, ids: set[str]) -> dict[str, int]:
    """
    응답 어디에 있든 '상품번호 -> 가격' 짝을 찾아낸다.
    응답 구조를 모르므로, 우리가 가진 상품번호와 맞는 값이 든 객체를 훑는다.
    """
    found: dict[str, int] = {}

    def walk(node: Any, depth: int = 0) -> None:
        if depth > 8 or len(found) >= len(ids):
            return
        if isinstance(node, list):
            for child in node:
                walk(child, depth + 1)
            return
        if not isinstance(node, dict):
            return

        item_id = None
        for value in node.values():
            if isinstance(value, (str, int)) and str(value).strip() in ids:
                item_id = str(value).strip()
                break
        if item_id:
            price = None
            for key, value in node.items():
                if str(value).strip() == item_id:
                    continue
                if any(w in key.lower() for w in ("price", "amt", "amount", "cost")):
                    candidate = to_int_price(value)
                    if candidate and candidate > 0:
                        price = candidate
                        break
            if price:
                found[item_id] = price
        for value in node.values():
            walk(value, depth + 1)

    walk(data)
    return found


# 요청 형식. 첫 줄이 옥션에서 실제로 쓰는 모양이고, 나머지는 대비책이다.
PRICE_API_SHAPES: list[tuple[str, str, str]] = [
    ("POST", "json_list", "itemNos"),
    ("POST", "json", "itemNos"),
    ("POST", "json", "itemNoList"),
    ("POST", "form", "itemNos"),
    ("GET", "query", "itemNos"),
]
# 한 번에 보낼 상품 개수
PRICE_API_CHUNK = 40


class LinkHarvestCollector(Collector):
    """
    서버가 HTML로 목록을 내려주는 페이지용. 상품 상세 링크 패턴으로 순위를 복원한다.

    CSS 클래스명은 사이트 개편 때마다 바뀌지만 상품 상세 URL 형태는 훨씬 오래 유지된다.
    그래서 클래스 대신 href 패턴을 기준으로 잡는다.
    """

    def __init__(
        self,
        *,
        key: str,
        label: str,
        source_url: str,
        id_pattern: str,
        base_url: str | None = None,
        link_base: str | None = None,
        interval: int = 600,
        limit: int = DEFAULT_LIMIT,
        min_title_len: int = 3,
        charset: str | None = None,
        warmup_url: str | None = None,
        price_api: str | None = None,
        enabled: bool = True,
        note: str | None = None,
        extra_headers: dict | None = None,
    ):
        self.key, self.label = key, label
        self.kind = "product"
        self.source_url = source_url
        self.id_re = re.compile(id_pattern, re.IGNORECASE)
        self.base_url = base_url or source_url
        # 목록 페이지와 상품 상세가 다른 도메인에 있는 경우가 있다(옥션이 그렇다).
        # 링크가 절대 주소면 이 값은 무시된다.
        self.link_base = link_base or self.base_url
        self.interval, self.limit = interval, limit
        self.min_title_len = min_title_len
        self.charset = charset
        self.warmup_url = warmup_url
        # 가격을 따로 받아오는 주소. 옥션의 쿠폰 적용가가 이런 경우다.
        self.price_api = price_api
        self.enabled, self.note = enabled, note
        self.extra_headers = extra_headers or {}

    TITLE_SELECTOR = (
        "[class*=title], [class*=Title], [class*=name], [class*=Name], "
        "[class*=goods], [class*=prd], [class*=product], [class*=item_t], strong, h3, h4"
    )

    # 제목 뒤에 딸려 오는 안내 문구. 여기서 끊는다.
    TAIL_RE = re.compile(r"(쿠폰적용가|무료배송|오늘출발|내일도착)")
    # 끝에 붙은 가격만 떼어낸다. 상품명 중간의 숫자는 건드리지 않는다.
    TRAILING_PRICE_RE = re.compile(r"(?:\s*[0-9]{1,3}(?:,[0-9]{3})+\s*원)+\s*$")

    @classmethod
    def _clean(cls, text: str | None) -> str | None:
        """
        상품명을 다듬는다.

        링크가 카드 전체를 감싸면 제목 뒤에 가격이나 배송 문구가 딸려 온다.
        그 부분만 떼어내되, 상품명 안의 퍼센트나 숫자는 그대로 둔다.
        '25% 라이트 200g' 같은 표기가 상품명의 일부이기 때문이다.
        """
        if not text:
            return None
        t = re.sub(r"\s+", " ", text).strip()

        m = cls.TAIL_RE.search(t)
        if m and m.start() > 0:
            t = t[: m.start()]

        # 끝에 가격이 여러 번 붙는 경우가 있어 없어질 때까지 떼어낸다
        while True:
            stripped = cls.TRAILING_PRICE_RE.sub("", t).strip()
            if stripped == t.strip():
                break
            t = stripped

        return t.strip(" -·,") or None

    def _title_of(self, anchor) -> str | None:
        """
        상품명을 찾는다. 후보를 모아 가장 긴 것을 고른다.
        먼저 찾은 것을 쓰면 '[스팸]스팸' 같은 짧은 딱지에 걸린다.

        후보는 같은 상품 카드 안에서만 모은다. 위로 올라가다 다른 상품 링크를
        만나면 거기서 멈춘다. 옆 상품 이름을 끌어오지 않기 위해서다.
        """
        img = anchor.find("img")

        # 제목 자리로 지정된 곳들. 이쪽을 우선한다.
        named: list[str | None] = [
            anchor.get("title"),
            img.get("alt") if img else None,
            img.get("title") if img else None,
            anchor.get("data-montelena-productname"),
        ]
        plain: list[str | None] = [anchor.get_text(" ", strip=True)]

        block = anchor
        for _ in range(3):
            block = block.parent
            if block is None or getattr(block, "name", None) is None:
                break

            # 이 덩어리가 여러 상품을 담고 있으면 더 올라가지 않는다
            ids = {
                m.group(1)
                for a in block.select("a[href]")
                if (m := self.id_re.search(a.get("href", "")))
            }
            if len(ids) > 1:
                break

            for node in block.select(self.TITLE_SELECTOR):
                named.append(node.get("title"))
                named.append(node.get_text(" ", strip=True))
            for other in block.select("a[href]"):
                named.append(other.get("title"))
                other_img = other.find("img")
                if other_img:
                    named.append(other_img.get("alt"))
                plain.append(other.get_text(" ", strip=True))

        for pool in (named, plain):
            cleaned = [t for t in (self._clean(c) for c in pool) if t and 3 <= len(t) <= 150]
            if cleaned:
                return max(cleaned, key=len)
        return None

    # 취소선이 그어진 원가가 들어 있는 자리
    STRIKE_TAGS = ("del", "s", "strike")
    STRIKE_WORDS = ("origin", "before", "through", "strike", "old", "list_price", "del")

    @classmethod
    def _is_struck(cls, tag) -> bool:
        node = tag
        for _ in range(4):
            if node is None or getattr(node, "name", None) is None:
                return False
            if node.name in cls.STRIKE_TAGS:
                return True
            classes = " ".join(node.get("class") or []).lower()
            if any(w in classes for w in cls.STRIKE_WORDS):
                return True
            node = node.parent
        return False

    @classmethod
    def _text_alive(cls, node) -> str:
        """취소선 친 글자는 빼고 읽는다. 원가를 가격으로 착각하지 않기 위해서다."""
        parts = [t for t in node.find_all(string=True) if not cls._is_struck(t.parent)]
        return re.sub(r"\s+", " ", " ".join(parts)).strip()

    PRICE_SELECTOR = (
        "[class*=price], [class*=Price], [class*=sale], [class*=Sale], "
        "[class*=cost], [class*=amount], [class*=won], strong"
    )

    @classmethod
    def _price_of(cls, anchor) -> tuple[int | None, str | None]:
        """
        가격을 찾는다. 쿠폰을 적용한 가격이 있으면 그것을 먼저 쓴다.

        사이트마다 '원'을 붙이는 자리도, 감싸는 깊이도 달라서
        링크 주변을 몇 단계 위까지 훑고, 가격처럼 보이는 요소도 따로 본다.
        돌려주는 값은 (가격, 표시할 말)이다.
        """
        node = anchor
        for _ in range(4):
            if node is None or getattr(node, "name", None) is None:
                break

            # 가격 전용 요소부터 본다. 취소선 친 원가는 건너뛴다.
            for tag in node.select(cls.PRICE_SELECTOR):
                if cls._is_struck(tag):
                    continue
                text = cls._text_alive(tag)
                price = parse_price(text) or parse_bare_number(text)
                if price:
                    return price, None

            price = parse_price(cls._text_alive(node))
            if price:
                return price, None
            node = node.parent
        return None, None

    # 이미지 주소가 들어갈 수 있는 자리. 늦게 불러오는 방식이 여럿이라 다 본다.
    IMG_ATTRS = (
        "src", "data-src", "data-original", "data-lazy", "data-lazy-src",
        "data-original-src", "data-echo", "data-url",
    )

    @classmethod
    def _image_of(cls, anchor) -> str | None:
        def pick(img) -> str | None:
            for attr in cls.IMG_ATTRS:
                value = img.get(attr)
                if value and not value.startswith("data:"):
                    return value
            srcset = img.get("srcset")
            if srcset:
                return srcset.split(",")[0].strip().split(" ")[0]
            return None

        img = anchor.find("img")
        src = pick(img) if img else None

        if not src:
            block = anchor
            for _ in range(3):
                block = block.parent
                if block is None or getattr(block, "name", None) is None:
                    break
                for candidate in block.find_all("img"):
                    src = pick(candidate)
                    if src:
                        break
                if src:
                    break
                # 배경 이미지로 넣는 경우도 있다
                for tag in block.find_all(style=True):
                    m = re.search(r"url\(['\"]?([^'\")]+)", tag["style"])
                    if m:
                        src = m.group(1)
                        break
                if src:
                    break

        if not src:
            return None
        return "https:" + src if src.startswith("//") else src

    @staticmethod
    def summarize_links(soup, top: int = 3) -> str:
        """항목을 못 뽑았을 때 어떤 링크가 있었는지 요약해 오류 메시지에 붙인다."""
        from collections import Counter
        from urllib.parse import parse_qs, urlparse

        shapes: Counter = Counter()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href or href.startswith(("#", "javascript:")):
                continue
            u = urlparse(href)
            path = re.sub(r"/\d{3,}", "/{숫자}", u.path)
            keys = ",".join(sorted(parse_qs(u.query).keys()))
            shapes[f"{path}?{keys}" if keys else path] += 1
        if not shapes:
            return "링크가 하나도 없습니다. 목록을 자바스크립트로 그리는 구조로 보입니다."
        parts = [f"{shape}({count}회)" for shape, count in shapes.most_common(top)]
        return "가장 많은 링크 형태: " + ", ".join(parts)

    async def _price_api_call(
        self, client: httpx.AsyncClient, headers: dict, shape: tuple[str, str, str], ids: list[str]
    ) -> dict[str, int]:
        method, kind, field = shape
        joined = ",".join(ids)
        if kind == "json_list":
            resp = await client.request(
                method, self.price_api,
                headers={**headers, "Content-Type": "application/json; charset=UTF-8"},
                json={"currentInputChannel": "", field: ids},
            )
        elif kind == "json":
            resp = await client.request(
                method, self.price_api,
                headers={**headers, "Content-Type": "application/json; charset=UTF-8"},
                json={field: joined},
            )
        elif kind == "form":
            resp = await client.request(method, self.price_api, headers=headers, data={field: joined})
        else:
            resp = await client.request(method, self.price_api, headers=headers, params={field: joined})
        resp.raise_for_status()
        return find_price_map(unwrap_asmx(resp.json()), set(ids))

    async def _apply_price_api(self, client: httpx.AsyncClient, items: list[Item]) -> None:
        """
        가격을 따로 내려주는 주소가 있으면 불러서 덮어쓴다.
        옥션의 쿠폰 적용가가 이런 경우다. 상품이 많으면 나눠 보낸다.
        """
        by_id: dict[str, Item] = {}
        for item in items:
            m = self.id_re.search(item.url or "")
            if m:
                by_id[m.group(1)] = item
        if not by_id:
            return

        ids = list(by_id)
        headers = {
            **BROWSER_HEADERS,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.source_url,
        }

        chunks = [ids[i : i + PRICE_API_CHUNK] for i in range(0, len(ids), PRICE_API_CHUNK)]
        shape = None
        tried: list[str] = []
        got: dict[str, int] = {}

        for chunk in chunks:
            # 한 번 통한 형식은 다음 묶음에도 그대로 쓴다
            order = [shape] if shape else PRICE_API_SHAPES
            for candidate in order:
                try:
                    prices = await self._price_api_call(client, headers, candidate, chunk)
                except Exception as exc:  # noqa: BLE001
                    if shape is None:
                        tried.append(f"{candidate[0]}/{candidate[1]}({exc.__class__.__name__})")
                    continue
                if prices:
                    got.update(prices)
                    shape = candidate
                    break
                if shape is None:
                    tried.append(f"{candidate[0]}/{candidate[1]}(짝을 못 찾음)")

        if not got:
            self.warning = (
                "쿠폰 적용가를 받지 못해 페이지에 적힌 금액을 씁니다. " + ", ".join(tried[:2])
            )
            return

        for item_id, price in got.items():
            by_id[item_id].price = price
        self.price_api_shape = f"{shape[0]}/{shape[1]}/{shape[2]}"

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        self.price_api_shape = None
        headers = {**BROWSER_HEADERS, **self.extra_headers}
        # 첫 방문처럼 메인 페이지를 먼저 열어 세션 쿠키를 받는다.
        # 목록 페이지를 곧바로 두드리면 막는 곳이 있어서 넣어 둔 절차다.
        if self.warmup_url:
            try:
                await client.get(self.warmup_url, headers=headers)
            except Exception:  # noqa: BLE001
                pass
        resp = await client.get(self.source_url, headers=headers)
        resp.raise_for_status()
        # 옥션처럼 아직 EUC-KR인 페이지가 있다. 인코딩을 지정하지 않으면 한글이 깨진다.
        html = decode_html(resp, self.charset)
        if looks_like_bot_wall(html):
            raise BotBlocked(
                "사이트가 자동 수집을 차단했습니다. 봇 확인 화면이 돌아왔습니다."
            )
        soup = BeautifulSoup(html, "html.parser")

        seen: set[str] = set()
        items: list[Item] = []
        matched = duplicated = untitled = 0
        for anchor in soup.select("a[href]"):
            href = anchor["href"]
            m = self.id_re.search(href)
            if not m:
                continue
            matched += 1
            pid = m.group(1)
            if pid in seen:
                duplicated += 1
                continue
            title = self._title_of(anchor)
            if not title or len(title) < self.min_title_len:
                untitled += 1
                continue
            seen.add(pid)
            price, price_note = self._price_of(anchor)
            items.append(
                Item(
                    rank=len(items) + 1,
                    title=title,
                    url=urllib.parse.urljoin(self.link_base, href),
                    price=price,
                    image=self._image_of(anchor),
                    meta=price_note,
                )
            )
            if len(items) >= self.limit:
                break

        if items and self.price_api:
            await self._apply_price_api(client, items)

        if items and not any(i.price for i in items):
            self.warning = "가격을 읽지 못했습니다. 페이지 구조가 바뀌었을 수 있습니다."

        if not items:
            if matched == 0:
                raise RuntimeError(
                    f"'{self.id_re.pattern}' 형태의 상품 링크가 없습니다. "
                    + self.summarize_links(soup)
                )
            raise RuntimeError(
                f"상품 링크 {matched}개를 찾았지만 이름을 뽑지 못했습니다 "
                f"(중복 {duplicated}개, 이름 없음 {untitled}개). "
                "링크에 글자가 없거나 한글이 깨졌을 수 있습니다."
            )
        return items


# ---------------------------------------------------------------- 페이지에 심긴 JSON


# 상품 목록에서 흔히 쓰이는 키 이름. 앞에 있을수록 우선한다.
TITLE_KEYS = (
    "productName", "goodsName", "itemName", "prdNm", "prdName",
    "name", "title", "keyword", "productTitle",
)
ID_KEYS = (
    "productId", "productNo", "prdNo", "goodsNo", "nvMid", "itemNo",
    "catalogId", "id", "no",
)
# 쿠폰을 적용한 가격. 있으면 이쪽을 먼저 쓴다.
COUPON_PRICE_KEYS = (
    "couponPrice", "cpnPrice", "couponAppliedPrice", "couponDscPrice",
    "couponDiscountPrice", "cardCouponPrice", "finalCouponPrice",
)
PRICE_KEYS = (
    "finalDscPrice", "discountedSalePrice", "finalPrice", "finalDiscountPrice",
    "salePrice", "sellPrice",
    "discountPrice", "lastDiscountAmt", "lowestPrice", "price",
)
IMAGE_KEYS = (
    "imageUrl", "productImageUrl", "thumbnailUrl", "imageUrl300", "imgUrl",
    "image", "thumbnail",
)
URL_KEYS = (
    "productUrl", "linkUrl1", "linkUrl", "mallProductUrl", "detailUrl", "url", "link",
)


def row_rank(row: dict) -> int | None:
    """행에 붙은 순위 번호를 꺼낸다. rankInfo.rank 처럼 한 겹 안에 있는 경우도 본다."""
    value = row.get("rankInfo")
    if isinstance(value, dict):
        value = value.get("rank")
    if value is None:
        value = row.get("rank")
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def is_advertisement(row: dict) -> bool:
    """광고 상품인지 본다. 11번가 '도전!베스트'처럼 순위와 무관한 자리가 있다."""
    for key in ("adYn", "isAd", "adYN"):
        value = row.get(key)
        if isinstance(value, str) and value.strip().upper() in {"Y", "TRUE"}:
            return True
        if value is True:
            return True
    return False


def _first_key(row: dict, candidates: Iterable[str]) -> str | None:
    lowered = {k.lower(): k for k in row}
    for c in candidates:
        if c.lower() in lowered:
            return lowered[c.lower()]
    return None


# Next.js 같은 프레임워크는 화면에 그릴 데이터를 HTML 안에 통째로 심어 둔다.
# 목록을 자바스크립트로 그리더라도 이 덩어리만 꺼내면 XHR 주소를 몰라도 된다.
EMBEDDED_JSON_PATTERNS = (
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>',
    r'<script[^>]*>\s*window\.__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*;?\s*</script>',
    r'<script[^>]*>\s*window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;?\s*</script>',
    r'<script[^>]*>\s*window\.__APOLLO_STATE__\s*=\s*(\{.*?\})\s*;?\s*</script>',
)


# Next.js 최신 구조(App Router)는 데이터를 이렇게 잘게 쪼개 넣는다.
FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[\d+\s*,\s*"((?:[^"\\]|\\.)*)"\s*\]\)')

_DECODER = json.JSONDecoder()


def extract_flight_text(html: str) -> str:
    """__next_f 조각들을 이어 붙여 원래 문자열로 되돌린다."""
    parts: list[str] = []
    for m in FLIGHT_RE.finditer(html):
        with contextlib.suppress(json.JSONDecodeError, ValueError):
            parts.append(json.loads('"' + m.group(1) + '"'))
    return "".join(parts)


def scan_json_blobs(text: str, max_attempts: int = 3000) -> list[Any]:
    """
    문자열 아무 데나 박혀 있는 JSON을 찾아낸다.
    상품 목록에 흔한 키가 가까이 있는 자리에서만 해석을 시도해 헛수고를 줄인다.
    """
    hints = tuple(f'"{k}"' for k in TITLE_KEYS)
    found: list[Any] = []
    attempts = 0
    i = 0
    while i < len(text) and attempts < max_attempts:
        ch = text[i]
        if ch not in "[{":
            i += 1
            continue
        window = text[i : i + 600]
        if not any(h in window for h in hints):
            i += 1
            continue
        attempts += 1
        try:
            obj, end = _DECODER.raw_decode(text, i)
        except (json.JSONDecodeError, ValueError):
            i += 1
            continue
        if isinstance(obj, (dict, list)):
            found.append(obj)
            i = end
        else:
            i += 1
    return found


def extract_embedded_json(html: str) -> list[Any]:
    """HTML 안에 심긴 JSON 덩어리를 모두 꺼낸다. 세 가지 방식을 차례로 시도한다."""
    found: list[Any] = []

    # 1) 통째로 심긴 형태
    for pattern in EMBEDDED_JSON_PATTERNS:
        for m in re.finditer(pattern, html, re.DOTALL):
            with contextlib.suppress(json.JSONDecodeError, ValueError):
                found.append(json.loads(m.group(1)))

    # 2) Next.js App Router 조각
    flight = extract_flight_text(html)
    if flight:
        found += scan_json_blobs(flight)

    # 3) 그래도 없으면 HTML 본문을 직접 훑는다
    if not found:
        found += scan_json_blobs(html)

    return found


def find_object_lists(node: Any, path: str = "", depth: int = 0) -> list[tuple[str, int, list[str]]]:
    """
    중첩 JSON을 훑어 '객체가 여러 개 든 배열'을 모두 찾는다.
    상품 목록은 거의 항상 이 형태라, 경로와 키 이름을 보면 어디를 써야 할지 바로 보인다.
    """
    if depth > 12:
        return []
    out: list[tuple[str, int, list[str]]] = []
    if isinstance(node, list):
        if len(node) >= 5 and isinstance(node[0], dict):
            out.append((path, len(node), list(node[0].keys())[:14]))
        # 배열 안 어디에 데이터가 들었는지 모른다.
        # 날짜 카드가 늘어선 경우 마지막이 최신이므로 뒤쪽도 반드시 살펴야 한다.
        if len(node) <= 64:
            indexes = range(len(node))
        else:
            indexes = list(range(5)) + list(range(len(node) - 5, len(node)))
        for idx in indexes:
            child = node[idx]
            if isinstance(child, (dict, list)):
                out += find_object_lists(child, f"{path}[{idx}]", depth + 1)
    elif isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, (dict, list)):
                out += find_object_lists(v, f"{path}.{k}" if path else k, depth + 1)
    return out


def _keyset(names: Iterable[str]) -> set[str]:
    return {n.lower() for n in names}


def score_product_keys(keys: Iterable[str]) -> int:
    """상품 목록다운 배열일수록 높은 점수를 준다. 배너나 카테고리 배열을 걸러내기 위한 것."""
    low = _keyset(keys)
    score = 0
    if low & _keyset(TITLE_KEYS):
        score += 2
    if low & _keyset(PRICE_KEYS):
        score += 1
    if low & _keyset(ID_KEYS):
        score += 1
    if low & _keyset(URL_KEYS):
        score += 1
    return score


def collect_product_rows(
    data: Any,
    list_path: str = "",
    limit: int = DEFAULT_LIMIT,
    report: list[str] | None = None,
    dedupe: bool = True,
) -> list[dict]:
    """
    JSON 어딘가에 흩어져 있는 상품 목록을 모은다.

    한 배열에 다 들어 있는 경우도 있고, 화면 구역마다 잘려 여러 배열로 나뉜 경우도 있다.
    후자를 위해 조건을 만족하는 배열을 순서대로 이어 붙이고 중복을 제거한다.
    """
    if list_path:
        with contextlib.suppress(Exception):
            rows = dig(data, list_path)
            if isinstance(rows, list):
                if report is not None:
                    report.append(list_path)
                return [r for r in rows if isinstance(r, dict)][:limit]
        return []

    candidates = find_object_lists(data)

    # 순위 번호가 붙은 행이 있으면 그것만 쓴다.
    # 광고 블록에는 순위가 없으므로 이 단계에서 자연스럽게 걸러진다.
    ranked: list[tuple[int, dict]] = []
    seen_rank: set[str] = set()
    for path, _length, _keys in candidates:
        with contextlib.suppress(Exception):
            rows = dig(data, path)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict) or is_advertisement(row):
                    continue
                rank = row_rank(row)
                if rank is None:
                    continue
                if not _first_key(row, TITLE_KEYS):
                    continue
                marker = f"{rank}"
                if marker in seen_rank:
                    continue
                seen_rank.add(marker)
                ranked.append((rank, row))
    if len(ranked) >= 5:
        ranked.sort(key=lambda pair: pair[0])
        if report is not None:
            report.append(f"순위가 붙은 행 {len(ranked)}개")
        return [row for _rank, row in ranked][:limit]

    for threshold in (4, 3, 2):
        picked: list[dict] = []
        seen: set[str] = set()
        for path, _length, keys in candidates:
            if score_product_keys(keys) < threshold:
                continue
            with contextlib.suppress(Exception):
                rows = dig(data, path)
                if not isinstance(rows, list):
                    continue
                if report is not None and rows:
                    report.append(f"{path} ({len(rows)}개, 점수 {score_product_keys(keys)})")
                for row in rows:
                    if not isinstance(row, dict) or is_advertisement(row):
                        continue
                    if dedupe:
                        marker = str(
                            row.get(_first_key(row, ID_KEYS) or "")
                            or row.get(_first_key(row, TITLE_KEYS) or "")
                        )
                        if not marker or marker in seen:
                            continue
                        seen.add(marker)
                    picked.append(row)
            if len(picked) >= limit:
                return picked[:limit]
        if picked:
            return picked[:limit]
    return []


def rows_to_items(rows: list[dict], *, url_template: str | None, title_key: str | None) -> list[Item]:
    """찾은 행들을 화면에 쓸 형태로 바꾼다. 키 이름은 흔한 후보들과 맞춰 본다."""
    if not rows:
        return []
    sample = rows[0]
    tkey = title_key or _first_key(sample, TITLE_KEYS)
    if not tkey:
        raise RuntimeError(f"제목에 해당하는 키를 찾지 못했습니다. 있는 키: {list(sample)[:12]}")
    ikey = _first_key(sample, ID_KEYS)
    ckey = _first_key(sample, COUPON_PRICE_KEYS)
    pkey = _first_key(sample, PRICE_KEYS)
    gkey = _first_key(sample, IMAGE_KEYS)
    ukey = _first_key(sample, URL_KEYS)

    items: list[Item] = []
    for row in rows:
        title = str(row.get(tkey) or "").strip()
        if not title:
            continue
        url = row.get(ukey) if ukey else None
        if not url and url_template and ikey and row.get(ikey) is not None:
            url = url_template.format(id=row[ikey])
        price = None
        meta = None
        if ckey and row.get(ckey) not in (None, "", 0):
            with contextlib.suppress(ValueError, TypeError):
                price = int(str(row[ckey]).replace(",", ""))
                meta = "쿠폰적용가"
        if price is None and pkey and row.get(pkey) is not None:
            with contextlib.suppress(ValueError, TypeError):
                price = int(str(row[pkey]).replace(",", ""))
        image = row.get(gkey) if gkey else None
        if isinstance(image, str) and image.startswith("//"):
            image = "https:" + image
        items.append(
            Item(rank=len(items) + 1, title=title, url=url, price=price,
                 image=image, meta=meta)
        )
    return items


class AutoJsonCollector(Collector):
    """
    JSON API 주소만 알면 되는 수집기.
    응답 구조를 몰라도 상품 목록으로 보이는 배열을 스스로 찾아낸다.
    """

    def __init__(
        self,
        *,
        key: str,
        label: str,
        source_url: str,
        api_url: str | list[str],
        kind: str = "product",
        list_path: str = "",
        title_key: str | None = None,
        url_template: str | None = None,
        headers: dict | None = None,
        interval: int = 600,
        limit: int = DEFAULT_LIMIT,
        enabled: bool = True,
        note: str | None = None,
    ):
        self.key, self.label, self.kind = key, label, kind
        self.source_url = source_url
        # 한 번에 안 오고 페이지가 나뉘는 API가 있다. 주소를 여러 개 주면 순서대로 이어 받는다.
        self.api_urls = [api_url] if isinstance(api_url, str) else list(api_url)
        self.list_path, self.title_key = list_path, title_key
        self.url_template, self.headers = url_template, headers or {}
        self.interval, self.limit, self.enabled, self.note = interval, limit, enabled, note
        self.picked_paths: list[str] = []

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        headers = {
            **BROWSER_HEADERS,
            "Accept": "application/json, text/plain, */*",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Referer": self.source_url,
            **self.headers,
        }
        rows: list[dict] = []
        seen: set[str] = set()
        last_data: Any = None
        failures: list[str] = []
        self.picked_paths: list[str] = []

        for page_no, url in enumerate(self.api_urls, start=1):
            if len(rows) >= self.limit:
                break
            try:
                resp = await client.get(url, headers=headers)
                resp.raise_for_status()
                last_data = resp.json()
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{page_no}쪽({exc.__class__.__name__})")
                continue

            for row in collect_product_rows(
                last_data, self.list_path, self.limit * 3, report=self.picked_paths
            ):
                marker = str(
                    row.get(_first_key(row, ID_KEYS) or "")
                    or row.get(_first_key(row, TITLE_KEYS) or "")
                )
                if not marker or marker in seen:
                    continue
                seen.add(marker)
                rows.append(row)
                if len(rows) >= self.limit:
                    break

        if not rows:
            if failures and last_data is None:
                raise RuntimeError("요청이 모두 실패했습니다. " + ", ".join(failures))
            found = sorted(find_object_lists(last_data), key=lambda c: -c[1])[:3] if last_data else []
            hint = (
                "가장 큰 배열: "
                + ", ".join(f"{p or '(최상위)'}[{n}개, 키 {'/'.join(k[:4])}]" for p, n, k in found)
                if found
                else "객체 배열이 하나도 없습니다."
            )
            raise RuntimeError("상품 목록으로 보이는 배열을 찾지 못했습니다. " + hint)

        if failures:
            self.warning = f"일부 페이지를 못 받았습니다: {', '.join(failures)}"
        elif len(rows) < self.limit:
            self.warning = f"{self.limit}개를 채우지 못하고 {len(rows)}개만 받았습니다."
        return rows_to_items(rows, url_template=self.url_template, title_key=self.title_key)


class EmbeddedJsonCollector(Collector):
    """
    페이지 HTML에 심긴 JSON에서 상품 목록을 찾아낸다.

    list_path 를 지정하면 그 자리를 쓰고, 비워 두면 가장 그럴듯한 배열을 스스로 고른다.
    어느 쪽으로 잡혔는지는 --inspect 로 확인할 수 있다.
    """

    def __init__(
        self,
        *,
        key: str,
        label: str,
        source_url: str,
        kind: str = "product",
        list_path: str = "",
        title_key: str | None = None,
        url_template: str | None = None,
        interval: int = 900,
        limit: int = DEFAULT_LIMIT,
        charset: str | None = None,
        enabled: bool = True,
        note: str | None = None,
    ):
        self.key, self.label, self.kind = key, label, kind
        self.source_url = source_url
        self.list_path, self.title_key = list_path, title_key
        self.url_template = url_template
        self.interval, self.limit = interval, limit
        self.charset, self.enabled, self.note = charset, enabled, note

    def _pick_list(self, blobs: list[Any]) -> list[dict]:
        best: list[dict] = []
        for blob in blobs:
            rows = collect_product_rows(blob, self.list_path, self.limit)
            if len(rows) > len(best):
                best = rows
        return best

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        resp = await client.get(self.source_url, headers=BROWSER_HEADERS)
        resp.raise_for_status()
        html = decode_html(resp, self.charset)
        if looks_like_bot_wall(html):
            raise BotBlocked("사이트가 자동 수집을 차단했습니다. 봇 확인 화면이 돌아왔습니다.")

        blobs = extract_embedded_json(html)
        if not blobs:
            has_flight = "__next_f" in html
            scripts = html.count("<script")
            raise RuntimeError(
                f"HTML 안에서 상품 데이터를 찾지 못했습니다. "
                f"(본문 {len(html):,}자, script {scripts}개, "
                f"Next.js 조각 {'있음' if has_flight else '없음'}) "
                "목록을 XHR로 따로 받아오는 구조로 보입니다."
            )
        rows = self._pick_list(blobs)
        if not rows:
            found: list[tuple[str, int, list[str]]] = []
            for blob in blobs:
                found += find_object_lists(blob)
            found.sort(key=lambda c: -c[1])
            hint = (
                "가장 큰 배열: "
                + ", ".join(f"{p or '(최상위)'}[{n}개, 키 {'/'.join(k[:4])}]" for p, n, k in found[:2])
                if found
                else "객체 배열이 하나도 없습니다."
            )
            raise RuntimeError(
                f"데이터 덩어리 {len(blobs)}개를 찾았지만 상품 목록으로 보이는 배열이 없습니다. " + hint
            )

        return rows_to_items(rows, url_template=self.url_template, title_key=self.title_key)


# 응답 안에서 기준 기간을 나타내는 흔한 키
DATE_KEYS = ("date", "period", "range", "dateTime", "datetime", "title", "endDate")


def _date_label(node: Any) -> str:
    if not isinstance(node, dict):
        return ""
    lowered = {k.lower(): k for k in node}
    for key in DATE_KEYS:
        value = node.get(lowered.get(key.lower(), ""))
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _parent_path(path: str) -> str:
    """'data[3].ranks' -> 'data[3]' 처럼 한 단계 위 경로를 만든다."""
    if "." in path:
        return path.rsplit(".", 1)[0]
    if path.endswith("]"):
        return path[: path.rindex("[")]
    return ""


def extract_keyword_names(data: Any) -> tuple[list[str], str]:
    """
    데이터랩 응답에서 검색어 목록과 기준 기간 표시를 꺼낸다.

    응답에 날짜별 카드가 여러 개 들어 있는 경우가 있다.
    화면이 보여주는 것은 가장 최근 카드이므로 마지막 것을 고른다.
    """
    top_label = _date_label(data)

    # 목록이 하나만 들어 있는 단순한 형태
    if isinstance(data, dict):
        for key in ("ranks", "data", "result", "list"):
            rows = data.get(key)
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                tkey = _first_key(rows[0], TITLE_KEYS)
                if tkey:
                    return [str(r[tkey]) for r in rows if r.get(tkey)], top_label

    # 여러 덩어리가 있으면 마지막(가장 최근) 것을 쓴다
    paths = [
        path
        for path, _n, keys in find_object_lists(data)
        if {k.lower() for k in keys} & {t.lower() for t in TITLE_KEYS}
    ]
    if paths:
        path = paths[-1]
        with contextlib.suppress(Exception):
            rows = dig(data, path)
            if isinstance(rows, list) and rows:
                tkey = _first_key(rows[0], TITLE_KEYS)
                if tkey:
                    label = _date_label(dig(data, _parent_path(path))) or top_label
                    return [str(r[tkey]) for r in rows if r.get(tkey)], label

    return [], top_label


RANK_PREFIX_RE = re.compile(r"^\s*(\d{1,3})\s*[.)위]?\s+")
# 숫자와 기호뿐인 항목은 순위표의 내용이 아니다. 날짜나 페이지 번호 목록을 걸러낸다.
MEANINGFUL_RE = re.compile(r"[^\d\s.,%~\-()\[\]/]")


def harvest_ranked_text(
    html: str, limit: int = DEFAULT_LIMIT, min_items: int = 5
) -> tuple[list[tuple[str, str | None]], str]:
    """
    상품이 아니라 이름만 줄줄이 있는 순위표에서 항목을 뽑는다.
    인기분야, 검색어 순위처럼 링크도 가격도 없는 목록이 이런 형태다.

    고르는 기준
      - 숫자와 기호뿐인 항목은 세지 않는다. 날짜 선택 드롭다운 같은 것에 속지 않기 위해서다.
      - 앞에 순위 번호가 붙은 목록을 크게 우대한다.
      - 항목이 링크를 달고 있으면 조금 더 쳐준다.
      - 점수가 같으면 뒤에 나온 것을 고른다. 날짜별 카드가 늘어선 경우 최근 것이 뒤에 온다.
    """
    soup = BeautifulSoup(html, "html.parser")
    best: list[tuple[str, str | None]] = []
    best_score = 0.0
    best_label = ""

    for parent in soup.find_all(True):
        kids = parent.find_all(["li", "a", "span", "td", "dd", "p"], recursive=False)
        if len(kids) < min_items:
            continue

        rows: list[tuple[str, str | None]] = []
        numbered = linked = 0
        for kid in kids:
            text = re.sub(r"\s+", " ", kid.get_text(" ", strip=True))
            if not text:
                continue
            if RANK_PREFIX_RE.match(text):
                numbered += 1
                text = RANK_PREFIX_RE.sub("", text)
            if not (1 <= len(text) <= 40):
                continue
            if not MEANINGFUL_RE.search(text):
                continue
            link = kid if kid.name == "a" else kid.find("a")
            href = link.get("href") if link else None
            if href:
                linked += 1
            rows.append((text, href))

        if len(rows) < min_items:
            continue
        score = numbered * 3 + linked * 0.5 + len(rows)
        if score >= best_score:
            best_score, best = score, rows
            heading = parent.find_previous(["strong", "h1", "h2", "h3", "h4", "caption"])
            best_label = re.sub(r"\s+", " ", heading.get_text(" ", strip=True))[:30] if heading else ""

    return best[:limit], best_label


class RankedListCollector(Collector):
    """
    순위 목록만 있는 페이지용. JSON이면 JSON으로, HTML이면 HTML로 읽는다.
    응답이 GET으로 안 오면 POST도 한 번 시도한다.
    """

    def __init__(
        self,
        *,
        key: str,
        label: str,
        source_url: str,
        api_url: str | None = None,
        kind: str = "keyword",
        list_path: str = "",
        title_key: str | None = None,
        link_template: str | None = None,
        post_data: dict | None = None,
        period: str = DEFAULT_PERIOD,
        cid: str | None = None,
        api_candidates: Iterable[str] | None = None,
        cids: Iterable[tuple[str, str]] | None = None,
        per_category: int = 10,
        interval: int = 6 * 3600,
        limit: int = DEFAULT_LIMIT,
        enabled: bool = True,
        note: str | None = None,
    ):
        self.key, self.label, self.kind = key, label, kind
        self.source_url = source_url
        self.api_url = api_url or source_url
        self.list_path, self.title_key = list_path, title_key
        self.link_template = link_template
        self.post_data = post_data
        self.period = period if period in PERIODS else DEFAULT_PERIOD
        self.cid = cid
        # 확인되지 않은 API 후보. 되면 쓰고, 안 되면 페이지 HTML로 물러선다.
        self.api_candidates = list(api_candidates) if api_candidates else []
        self.cids = list(cids) if cids else None
        self.per_category = per_category
        self.interval, self.limit, self.enabled, self.note = interval, limit, enabled, note
        self.base_note = note
        self.picked_paths: list[str] = []

    def _link(self, name: str, href: str | None) -> str | None:
        if href:
            return urllib.parse.urljoin(self.source_url, href)
        if self.link_template:
            return self.link_template.format(q=urllib.parse.quote(name))
        return naver_shopping_search(name)

    # 데이터랩은 요청이 몰리면 막는다. 분야별 인기 검색어와 같은 속도로 맞춘다.
    CONCURRENCY = 2
    MIN_GAP = 0.8

    def _headers(self) -> dict[str, str]:
        return {
            **BROWSER_HEADERS,
            "Accept": "application/json, text/javascript, text/html, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Referer": self.source_url,
        }

    async def _fetch_one(
        self, client: httpx.AsyncClient, cid: str | None, limit: int
    ) -> tuple[list[tuple[str, str | None]] | list[str], str, list[str]]:
        """한 분야를 받아 순위 목록과 어디서 읽었는지를 돌려준다."""
        headers = self._headers()

        # 기간과 분야를 실어 보낸다. 파라미터 이름이 확실치 않아 둘 다 넣는다.
        params: dict[str, str] = {"timeUnit": self.period, "timeDimension": self.period}
        if cid:
            params["cid"] = cid
        if self.post_data:
            params.update(self.post_data)

        # API 후보를 먼저 두드려 보고, 안 되면 페이지 HTML을 읽는다.
        attempts: list[tuple[str, str, dict | None, dict | None]] = [
            ("GET", url, params, None) for url in self.api_candidates
        ] + [
            ("GET", self.api_url, params, None),
            ("POST", self.api_url, None, params),
            ("GET", self.api_url, None, None),
        ]

        errors: list[str] = []
        for method, url, query, data in attempts:
            try:
                resp = await client.request(
                    method, url, headers=headers, params=query, data=data
                )
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{url.rsplit('/', 1)[-1]}({exc.__class__.__name__})")
                continue

            body = decode_html(resp, None)
            if looks_like_bot_wall(body):
                raise BotBlocked("사이트가 자동 수집을 차단했습니다. 봇 확인 화면이 돌아왔습니다.")

            # JSON 이면 JSON 으로 읽는다
            with contextlib.suppress(Exception):
                data_json = resp.json()
                names, label = extract_keyword_names(data_json)
                if names:
                    where = url.rsplit("/", 1)[-1] + (f", {label}" if label else "")
                    return [(n, None) for n in names[:limit]], where, errors
                plain = self._names_from_json(data_json)
                if plain:
                    return [(n, None) for n in plain[:limit]], url.rsplit("/", 1)[-1], errors

            # 아니면 HTML 순위 목록으로 읽는다
            rows_text, label = harvest_ranked_text(body, limit)
            if rows_text:
                where = f"HTML 순위 목록({method})"
                return rows_text, f"{where}, {label}" if label else where, errors
            errors.append(f"{url.rsplit('/', 1)[-1]}(목록 없음, 본문 {len(body):,}자)")

        return [], "", errors

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        self.picked_paths = []

        # 분야가 하나면 그대로, 여러 개면 분야마다 나눠 받아 묶는다
        if not self.cids:
            rows, where, errors = await self._fetch_one(client, self.cid, self.limit)
            if not rows:
                raise RuntimeError("순위 목록을 찾지 못했습니다. " + ", ".join(errors))
            if where:
                self.picked_paths.append(where)
                label = where.split(", ", 1)[-1]
                if self.base_note and label != where:
                    self.note = f"{self.base_note}, {label} 기준"
            return [
                Item(rank=i, title=name, url=self._link(name, href))
                for i, (name, href) in enumerate(rows, start=1)
            ]

        sem = asyncio.Semaphore(self.CONCURRENCY)
        gate = asyncio.Lock()
        loop = asyncio.get_running_loop()
        last_sent = [loop.time() - self.MIN_GAP]

        async def one(cid: str, name: str):
            async with sem:
                async with gate:
                    wait = self.MIN_GAP - (loop.time() - last_sent[0])
                    if wait > 0:
                        await asyncio.sleep(wait)
                    last_sent[0] = loop.time()
                try:
                    rows, where, errors = await self._fetch_one(client, cid, self.per_category)
                except BotBlocked:
                    raise
                except Exception as exc:  # noqa: BLE001
                    return name, [], "", [f"{exc.__class__.__name__}"]
            return name, rows, where, errors

        gathered = await asyncio.gather(*(one(cid, name) for cid, name in self.cids))

        items: list[Item] = []
        failed: list[str] = []
        seen_where = ""
        for name, rows, where, errors in gathered:
            if not rows:
                failed.append(name)
                continue
            seen_where = seen_where or where
            for i, (title, href) in enumerate(rows, start=1):
                items.append(Item(rank=i, title=title, url=self._link(title, href), group=name))

        if not items:
            raise RuntimeError("모든 분야에서 순위 목록을 찾지 못했습니다.")
        if seen_where:
            self.picked_paths.append(seen_where)
            label = seen_where.split(", ", 1)[-1]
            if self.base_note and label != seen_where:
                self.note = f"{self.base_note}, {label} 기준"
        if failed:
            self.warning = f"{len(failed)}개 분야를 못 받았습니다: {', '.join(failed)}"
        return items

    @staticmethod
    def _names_from_json(data: Any, depth: int = 0) -> list[str]:
        """['원피스', '블라우스', ...] 처럼 문자열만 든 배열도 순위표일 수 있다."""
        if depth > 10:
            return []
        if isinstance(data, list):
            strings = [x for x in data if isinstance(x, str) and 1 <= len(x) <= 40]
            if len(strings) >= 5 and len(strings) == len(data):
                return strings
            for child in data[:5]:
                found = RankedListCollector._names_from_json(child, depth + 1)
                if found:
                    return found
        elif isinstance(data, dict):
            for value in data.values():
                found = RankedListCollector._names_from_json(value, depth + 1)
                if found:
                    return found
        return []


class ChainCollector(Collector):
    """
    여러 수집 방식을 차례로 시도하고, 처음으로 항목을 얻은 방식을 쓴다.

    페이지가 심긴 JSON을 주는지 서버 렌더링 HTML을 주는지 미리 알 수 없을 때,
    한쪽으로 찍지 않고 둘 다 시도하면 된다.
    """

    def __init__(
        self,
        *,
        key: str,
        label: str,
        source_url: str,
        steps: list[Collector],
        kind: str = "product",
        interval: int = 600,
        enabled: bool = True,
        note: str | None = None,
    ):
        self.key, self.label, self.kind = key, label, kind
        self.source_url = source_url
        self.steps = steps
        self.interval, self.enabled, self.note = interval, enabled, note

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        failures: list[str] = []
        for step in self.steps:
            try:
                items = await step.fetch(client)
            except BotBlocked:
                raise
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{type(step).__name__}: {exc}")
                continue
            if items:
                return items
            failures.append(f"{type(step).__name__}: 항목 없음")
        raise RuntimeError(" / ".join(failures) if failures else "시도할 방식이 없습니다.")


# ---------------------------------------------------------------- 네이버 데이터랩


# 쇼핑인사이트 1차 분류 코드
NAVER_CIDS: list[tuple[str, str]] = [
    ("50000000", "패션의류"),
    ("50000001", "패션잡화"),
    ("50000002", "화장품/미용"),
    ("50000003", "디지털/가전"),
    ("50000004", "가구/인테리어"),
    ("50000005", "출산/육아"),
    ("50000006", "식품"),
    ("50000007", "스포츠/레저"),
    ("50000008", "생활/건강"),
    ("50000009", "여가/생활편의"),
    ("50005542", "도서"),
]


class NaverDatalabKeyword(Collector):
    """
    데이터랩 쇼핑인사이트의 분야별 인기 검색어.

    페이지가 쓰는 API를 그대로 부른다. timeUnit 만 바꾸면 일간/주간/월간이 된다.
    응답이 안 오면 예전 방식(구간을 직접 넘기는 API)으로 한 번 더 시도한다.
    """

    key = "naver_datalab_keyword"
    label = "네이버 데이터랩 분야별 인기 검색어"
    kind = "keyword"
    source_url = "https://datalab.naver.com/"
    interval = 6 * 3600
    note = "분야마다 상위 10개"

    API = "https://datalab.naver.com/shoppingInsight/getKeywordRank.naver"
    FALLBACK_API = "https://datalab.naver.com/shoppingInsight/getCategoryKeywordRank.naver"

    # 요청이 몰리면 429가 돌아온다. 발사 간격을 벌리는 쪽이 확실하다.
    CONCURRENCY = 2
    MIN_GAP = 0.8
    BACKOFF = (4, 10, 20)

    def __init__(
        self,
        cids: Iterable[tuple[str, str]] | None = None,
        per_category: int = 10,
        period: str = DEFAULT_PERIOD,
    ):
        self.cids = list(cids) if cids else NAVER_CIDS
        self.per_category = per_category
        self.period = period if period in PERIODS else DEFAULT_PERIOD
        self.base_note = self.note
        self.picked_paths: list[str] = []

    def _headers(self) -> dict[str, str]:
        return {
            **BROWSER_HEADERS,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Referer": self.source_url,
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://datalab.naver.com",
        }

    def _fallback_payload(self, cid: str) -> dict[str, str]:
        end = (now_kst() - timedelta(days=1)).date()
        span = {"date": 0, "week": 6, "month": 29}[self.period]
        return {
            "cid": cid,
            "timeUnit": self.period,
            "startDate": (end - timedelta(days=span)).isoformat(),
            "endDate": end.isoformat(),
            "age": "",
            "gender": "",
            "device": "",
            "page": "1",
            "count": str(self.per_category),
        }

    async def fetch(self, client: httpx.AsyncClient) -> list[Item]:
        self.picked_paths = []
        headers = self._headers()

        sem = asyncio.Semaphore(self.CONCURRENCY)
        gate = asyncio.Lock()
        loop = asyncio.get_running_loop()
        last_sent = [loop.time() - self.MIN_GAP]

        async def send(method: str, url: str, **kwargs) -> httpx.Response:
            """요청이 몰리지 않도록 발사 시점만 줄 세운다. 응답 대기는 겹쳐도 된다."""
            async with gate:
                wait = self.MIN_GAP - (loop.time() - last_sent[0])
                if wait > 0:
                    await asyncio.sleep(wait)
                last_sent[0] = loop.time()
            return await client.request(method, url, headers=headers, **kwargs)

        async def one(cid: str, name: str) -> tuple[str, list[str], str, str | None]:
            last = "알 수 없는 오류"
            for attempt in range(len(self.BACKOFF) + 1):
                async with sem:
                    try:
                        resp = await send(
                            "GET", self.API, params={"timeUnit": self.period, "cid": cid}
                        )
                        if resp.status_code == 429:
                            last = "HTTP 429"
                        else:
                            resp.raise_for_status()
                            names, label = extract_keyword_names(resp.json())
                            if names:
                                return name, names[: self.per_category], label, None
                            last = "빈 응답"
                            break
                    except httpx.HTTPStatusError as exc:
                        last = f"HTTP {exc.response.status_code}"
                        break
                    except Exception as exc:  # noqa: BLE001
                        last = f"{exc.__class__.__name__}"
                        break
                if attempt >= len(self.BACKOFF):
                    break
                await asyncio.sleep(self.BACKOFF[attempt])

            # 예전 방식으로 한 번 더
            async with sem:
                try:
                    resp = await send("POST", self.FALLBACK_API, data=self._fallback_payload(cid))
                    resp.raise_for_status()
                    names, label = extract_keyword_names(resp.json())
                    if names:
                        return name, names[: self.per_category], label, None
                except Exception as exc:  # noqa: BLE001
                    return name, [], "", f"{last}, 예전 방식도 {exc.__class__.__name__}"
            return name, [], "", last

        gathered = await asyncio.gather(*(one(cid, name) for cid, name in self.cids))

        items: list[Item] = []
        errors: list[str] = []
        label = ""
        for name, names, got_label, err in gathered:
            if err or not names:
                errors.append(f"{name}({err or '빈 응답'})")
                continue
            label = label or got_label
            for i, keyword in enumerate(names, start=1):
                items.append(
                    Item(
                        rank=i,
                        title=keyword,
                        url=naver_shopping_search(keyword),
                        group=name,
                    )
                )

        if not items:
            raise RuntimeError("모든 분야 요청 실패. " + ", ".join(errors[:3]))
        if label:
            self.note = f"{self.base_note}, {label} 기준"
            self.picked_paths.append(f"getKeywordRank ({PERIODS[self.period]}, {label})")
        if errors:
            missed = ", ".join(e.split("(")[0] for e in errors)
            self.warning = f"{len(errors)}개 분야를 못 받았습니다: {missed}"
        return items


# ---------------------------------------------------------------- 소스 목록


def build_collectors() -> list[Collector]:
    """
    확인된 것은 구현해 두고, 확인이 필요한 것은 껍데기만 두었다.
    enabled=False 인 항목은 화면에 '설정 필요' 상태로 표시된다.
    """
    return [
        # ---- 1. 네이버 데이터랩 분야별 인기 검색어 (구현됨)
        NaverDatalabKeyword(),

        # ---- 2. 네이버 데이터랩 인기분야
        # 분야 이름과 순위만 있는 목록이라 상품 수집기와 다르게 읽는다.
        RankedListCollector(
            key="naver_section",
            label="네이버 데이터랩 인기분야",
            source_url="https://datalab.naver.com/home/sectionSearch.naver",
            kind="keyword",
            interval=6 * 3600,
            cids=NAVER_CIDS,
            # 분야별 인기 검색어와 짝이 될 법한 주소. 아니면 페이지 HTML로 물러선다.
            api_candidates=["https://datalab.naver.com/shoppingInsight/getCategoryRank.naver"],
            note="분야마다 상위 10개",
        ),

        # ---- 3. 네이버쇼핑 많이 구매한 BEST
        # 페이지 HTML에 심긴 데이터를 꺼내 쓴다. 목록 배열은 스스로 찾는다.
        EmbeddedJsonCollector(
            key="snx_best",
            label="네이버쇼핑 많이 구매한 BEST",
            source_url=(
                "https://snxbest.naver.com/product/best/buy"
                "?ageType=ALL&categoryId=A&sortType=PRODUCT_BUY&periodType=DAILY"
            ),
            kind="product",
            url_template="https://shopping.naver.com/window-products/catalog/{id}",
            interval=1200,
            limit=PRODUCT_LIMIT,
            note="일간 구매 기준 전체 카테고리",
        ),

        # ---- 4. 11번가 BEST
        # 페이지는 빈 껍데기라 목록 API를 직접 부른다. 응답 구조는 자동으로 찾는다.
        AutoJsonCollector(
            key="elevenst_best",
            label="11번가 BEST",
            source_url="https://www.11st.co.kr/page/best",
            api_url=[
                "https://apis.11st.co.kr/pui/v2/page?pageId=PCBEST",
                "https://apis.11st.co.kr/pui/v2/page"
                "?pageId=PCBEST&blckSn=34975&pageMode=NEXT&pageNo=2",
            ],
            kind="product",
            url_template="https://www.11st.co.kr/products/{id}",
            interval=1200,
            limit=PRODUCT_LIMIT,
            note="전체 베스트 상위 100개",
        ),

        # ---- 5. 옥션 BEST (링크 패턴 기반, 검증 필요)
        LinkHarvestCollector(
            key="auction_best",
            label="옥션 BEST",
            source_url="http://corners.auction.co.kr/corner/CategoryBest.aspx",
            id_pattern=r"[Ii]tem[Nn]o=([A-Za-z0-9]+)",
            base_url="http://corners.auction.co.kr/",
            link_base="https://itempage3.auction.co.kr/",
            charset="euc-kr",
            warmup_url="https://www.auction.co.kr/",
            price_api="https://corners.auction.co.kr/Best/BestWebService.asmx/GetCouponAppliedPrice",
            note="전체 베스트 상위 100개",
            interval=1200,
            limit=PRODUCT_LIMIT,
        ),

    ]
