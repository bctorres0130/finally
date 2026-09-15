# Market Data Backend — Detailed Design

> **Status of this document:** The market data subsystem described here is **implemented, tested, and merged** at `backend/app/market/`. This document is the detailed reference for it — how it works, why it's built this way, and (in §10–11) how the still-to-be-built parts of the backend (FastAPI app assembly, watchlist routes, portfolio valuation) should integrate with it. See `planning/MARKET_DATA_SUMMARY.md` for the short version and `planning/archive/` for the original pre-implementation proposal this was built from.

## Table of Contents

1. [File Structure](#1-file-structure)
2. [Data Model — `PriceUpdate`](#2-data-model--priceupdate)
3. [Price Cache](#3-price-cache)
4. [Abstract Interface — `MarketDataSource`](#4-abstract-interface--marketdatasource)
5. [Seed Prices & Ticker Parameters](#5-seed-prices--ticker-parameters)
6. [GBM Simulator](#6-gbm-simulator)
7. [Massive API Client](#7-massive-api-client)
8. [Factory](#8-factory)
9. [SSE Streaming Endpoint](#9-sse-streaming-endpoint)
10. [FastAPI Lifecycle Integration (forward-looking)](#10-fastapi-lifecycle-integration-forward-looking)
11. [Watchlist Coordination (forward-looking)](#11-watchlist-coordination-forward-looking)
12. [Testing Strategy](#12-testing-strategy)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Configuration Summary](#14-configuration-summary)

---

## 1. File Structure

```
backend/
├── app/
│   ├── __init__.py
│   └── market/
│       ├── __init__.py         # Public exports
│       ├── models.py            # PriceUpdate dataclass
│       ├── cache.py             # PriceCache (thread-safe store)
│       ├── interface.py         # MarketDataSource ABC
│       ├── seed_prices.py       # Seed prices, GBM params, correlation groups
│       ├── simulator.py         # GBMSimulator + SimulatorDataSource
│       ├── massive_client.py    # MassiveDataSource (Polygon.io via `massive`)
│       ├── factory.py           # create_market_data_source()
│       └── stream.py            # create_stream_router() — SSE endpoint
├── tests/
│   └── market/                  # 73 tests, 84% coverage overall
├── market_data_demo.py          # Rich terminal live-dashboard demo
└── pyproject.toml
```

Public surface (`app/market/__init__.py`):

```python
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Everything downstream (portfolio valuation, trade execution, watchlist routes, the SSE endpoint) imports **only** from `app.market` — never from the internal modules directly. This keeps the simulator/Massive split invisible outside the package.

---

## 2. Data Model — `PriceUpdate`

An immutable, frozen dataclass. One instance = one ticker's price at one instant.

```python
# app/market/models.py
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True, slots=True`** — immutable and memory-lean. A `PriceUpdate` is a value, not a mutable record; once written to the cache it's never mutated, only replaced.
- **`change`/`change_percent`/`direction` are derived properties**, not stored fields — there's exactly one source of truth (`price`, `previous_price`), so they can never drift out of sync.
- **`to_dict()`** is the single serialization boundary — both the SSE stream (§9) and any future REST endpoint (`GET /api/watchlist`) call this, so the wire format only needs to change in one place.
- **`timestamp` defaults to `time.time()`** at construction so tests and ad-hoc usage don't need to pass one, but `PriceCache.update()` always passes an explicit value it controls.

---

## 3. Price Cache

The single point of truth prices flow through. Producers (`SimulatorDataSource` / `MassiveDataSource`) write; consumers (SSE stream, portfolio valuation, trade execution) read. Nothing outside `app/market` talks to a data source directly.

```python
# app/market/cache.py
class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price   # first tick: flat

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None: ...
    def get_all(self) -> dict[str, PriceUpdate]: ...      # shallow copy, safe to iterate
    def get_price(self, ticker: str) -> float | None: ...  # convenience float accessor
    def remove(self, ticker: str) -> None: ...

    @property
    def version(self) -> int: ...   # for SSE change detection

    def __len__(self) -> int: ...
    def __contains__(self, ticker: str) -> bool: ...
```

### Why a version counter?

The SSE loop (§9) needs to know "did anything change since I last sent a payload?" without diffing dictionaries. A monotonically increasing integer, bumped inside the same lock as the write, is the cheapest possible answer: compare `cache.version` to the last value you saw; if it moved, something changed, full stop.

### Thread safety rationale

`SimulatorDataSource` writes from an `asyncio` task; a future `MassiveDataSource`-style REST call could in principle run in a worker thread (`asyncio.to_thread`, see §7). A plain `threading.Lock` protects the dict and the counter uniformly regardless of which context is writing. Reads are cheap (`dict` copy under the lock) so contention is a non-issue at this scale (≤ tens of tickers, 2 writes/sec).

---

## 4. Abstract Interface — `MarketDataSource`

Both implementations (`SimulatorDataSource`, `MassiveDataSource`) share one contract, so the rest of the app is agnostic to which one is running.

```python
# app/market/interface.py
class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates; must be called exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task; safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """No-op if already present. Included on the next update cycle."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """No-op if absent. Also removes the ticker from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

Lifecycle contract callers must follow:

```python
source = create_market_data_source(cache)
await source.start(["AAPL", "GOOGL", ...])
# ... app runs ...
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")
# ... app shutting down ...
await source.stop()
```

### Why the source writes to the cache instead of returning prices

If `step()`/`poll()` returned prices to a caller that then wrote them to the cache, every call site would need to know about both the source and the cache, and the polling cadence would leak out of the source. Instead each source owns its own `asyncio.Task` loop and the *only* thing it does with a new price is call `cache.update(...)`. This is why `start()` takes tickers but returns nothing, and why the interface has no `get_price()` method — reads never go through the source, only through the cache.

---

## 5. Seed Prices & Ticker Parameters

Static data driving both realism (Massive-free demo) and the simulator's correlation structure.

```python
# app/market/seed_prices.py
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

# sigma: annualized volatility · mu: annualized drift
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # high volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # high volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},    # low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},      # low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}  # for dynamically-added tickers

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # tech stocks move together
INTRA_FINANCE_CORR = 0.5   # finance stocks move together
CROSS_GROUP_CORR = 0.3     # between sectors / unknown tickers
TSLA_CORR = 0.3            # TSLA does its own thing
```

A ticker added at runtime via the AI chat or watchlist UI that isn't in `SEED_PRICES`/`TICKER_PARAMS` falls back to a random price in `[50, 300)` and `DEFAULT_PARAMS` — see `GBMSimulator._add_ticker_internal` in §6.1.

---

## 6. GBM Simulator

### 6.1 `GBMSimulator` — The Math Engine

Geometric Brownian Motion with Cholesky-correlated shocks across tickers, plus occasional random "event" jumps for visual drama.

```python
# app/market/simulator.py
class GBMSimulator:
    """
    S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ~8.48e-8 (one 500ms tick)

    def __init__(self, tickers, dt=DEFAULT_DT, event_probability=0.001):
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Hot path — called every 500ms. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # ~0.1% chance per tick per ticker → a 2-5% shock, either direction
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """O(n^2), n < 50 in practice — called only on add/remove, not per tick."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

**Why the tiny `dt`:** at 500ms ticks over a 252-trading-day year, `dt ≈ 8.5e-8`. Plugged into the GBM formula this produces realistic sub-cent-per-tick moves that compound into believable multi-percent moves over minutes — the simulator doesn't need any explicit "smoothing," the math does it.

**Why Cholesky, and why rebuild only on add/remove:** correlated normal draws are generated as `L @ z` where `L` is the lower-triangular Cholesky factor of the correlation matrix. Recomputing `L` is O(n³) via `np.linalg.cholesky`, so it's done once per topology change (ticker added/removed), never per tick — the hot `step()` path is just one matrix-vector multiply plus a per-ticker `exp()`.

### 6.2 `SimulatorDataSource` — Async Wrapper

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5, event_probability: float = 0.001):
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for ticker in tickers:                      # seed cache so SSE has data immediately
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)   # seed immediately

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")   # never let one bad tick kill the loop
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding** — both `start()` and `add_ticker()` write a price to the cache *before* the next scheduled tick, so a client that subscribes to SSE (or a watchlist add that happens between ticks) never sees a gap.
- **The loop never dies from a bad step** — `step()` is wrapped in `try/except Exception`, logged, and the loop continues; only `asyncio.CancelledError` (from `stop()`) actually ends it.
- **Update cadence is decoupled from ticker count** — one `step()` call advances every tracked ticker in one `numpy` vector op, so adding tickers doesn't change the tick interval.

---

## 7. Massive API Client

`MassiveDataSource` polls Polygon.io's snapshot endpoint (via the `massive` package) on a timer instead of using a persistent stream — REST polling works on every pricing tier, including the free one.

```python
# app/market/massive_client.py
class MassiveDataSource(MarketDataSource):
    """
    Free tier: 5 req/min → poll every 15s (default).
    Paid tiers: higher limits → poll every 2-5s.
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0):
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()          # immediate first poll — cache has data right away
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)   # appears on next poll, not immediately (unlike simulator)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous — run in a thread so it never blocks the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0   # ms → s
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # don't re-raise — common failures (401 bad key, 429 rate limit, network) self-heal
            # on the next interval; a single bad poll must not kill the background task

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Error handling philosophy

A poll failure (bad key, rate limit, transient network error) is logged and swallowed; the loop keeps running and simply tries again next interval. There is deliberately no retry-with-backoff or circuit breaker here — the polling interval itself *is* the backoff, and a persistently failing key just means stale prices in the cache (visible to the user as a stalled watchlist), not a crashed backend.

### `asyncio.to_thread` for the sync client

The `massive` package's `RESTClient` makes blocking HTTP calls. Wrapping the single `get_snapshot_all` call in `asyncio.to_thread` keeps the event loop free for SSE clients and other requests while the poll is in flight, without needing an async HTTP client or a separate process.

---

## 8. Factory

The only place that reads `MASSIVE_API_KEY` — everything else is unaware which source is active.

```python
# app/market/factory.py
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """
    MASSIVE_API_KEY set and non-empty → MassiveDataSource (real data)
    Otherwise                         → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

`.strip()` matters: an `.env` file with `MASSIVE_API_KEY=` (present but empty, or with trailing whitespace) must fall through to the simulator, not attempt to construct a `RESTClient` with a blank key.

---

## 9. SSE Streaming Endpoint

```python
# app/market/stream.py
router = APIRouter(prefix="/api/stream", tags=["streaming"])

def create_stream_router(price_cache: PriceCache) -> APIRouter:
    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # disable nginx buffering if proxied
            },
        )
    return router

async def _generate_events(price_cache: PriceCache, request: Request, interval: float = 0.5):
    yield "retry: 1000\n\n"   # browser EventSource auto-reconnects after 1s on drop

    last_version = -1
    try:
        while True:
            if await request.is_disconnected():
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
```

### SSE wire format

Each event is the **full current snapshot** of every tracked ticker, keyed by symbol:

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.53, "previous_price": 190.41, "timestamp": 1737654321.123, "change": 0.12, "change_percent": 0.063, "direction": "up"}, "GOOGL": {...}, ...}

```

The frontend's single `EventSource("/api/stream/prices")` listener therefore gets one `onmessage` per changed tick, with every ticker's latest state — no per-ticker subscription management needed, and the sparkline accumulation described in `PLAN.md` §2/§10 is just "append each incoming point to that ticker's local array."

### Why poll-and-push instead of event-driven

The cache has no built-in pub/sub — adding one (e.g. an `asyncio.Condition` per ticker) would add real complexity for no visible benefit, since the simulator/Massive poller already updates on a fixed ~500ms cadence. Comparing `price_cache.version` every `interval` seconds is O(1) and only serializes a JSON payload when something actually changed, which is the only optimization that matters here (avoiding wasted `dumps()` calls on quiet ticks — Massive's 15s poll interval, in particular, would otherwise mean sending 30 identical payloads between real updates).

### Client disconnect detection

`request.is_disconnected()` is checked every loop iteration (not just relied on via `CancelledError`), because Starlette's disconnect detection depends on periodically checking rather than an event firing at exactly the moment of disconnect. This is why the loop always sleeps `interval` seconds even when there's no new data to send — it doubles as the disconnect poll cadence.

---

## 10. FastAPI Lifecycle Integration (forward-looking)

`app/main.py` does not exist yet — this section is guidance for the agent who builds it, wiring the market data subsystem into the FastAPI app's startup/shutdown and making the cache reachable from other routers.

```python
# app/main.py (to be built)
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router
from app.db import get_watchlist_tickers   # to be built — reads seeded watchlist from SQLite

price_cache = PriceCache()

@asynccontextmanager
async def lifespan(app: FastAPI):
    source = create_market_data_source(price_cache)
    tickers = get_watchlist_tickers(user_id="default")   # lazy-inits DB + seed data if needed
    await source.start(tickers)
    app.state.market_source = source
    app.state.price_cache = price_cache
    yield
    await source.stop()

app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
# app.include_router(portfolio_router)
# app.include_router(watchlist_router)
# app.include_router(chat_router)
```

### Accessing market data from other routes

Prefer a small FastAPI dependency over importing the module-level `price_cache` directly, so route handlers stay testable with an injected fake cache:

```python
# app/dependencies.py (to be built)
from fastapi import Request

from app.market import PriceCache

def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache

def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

```python
# app/portfolio/routes.py (to be built)
@router.get("/api/portfolio")
async def get_portfolio(cache: PriceCache = Depends(get_price_cache)):
    positions = load_positions(user_id="default")
    for pos in positions:
        pos.current_price = cache.get_price(pos.ticker)  # None if ticker somehow uncached
        ...
```

Portfolio valuation, trade execution (`POST /api/portfolio/trade`), and the LLM's structured trade actions (`planning/PLAN.md` §9) all read the *same* cache this way — there is no separate "get me a quote" RPC; the price used to fill a market order is simply `cache.get_price(ticker)` at request time.

---

## 11. Watchlist Coordination (forward-looking)

`app/watchlist/routes.py` does not exist yet either. The contract it must honor with the market data subsystem:

### Flow: Adding a ticker

```python
# app/watchlist/routes.py (to be built)
@router.post("/api/watchlist")
async def add_to_watchlist(
    body: AddTickerRequest,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = body.ticker.upper().strip()
    insert_watchlist_row(user_id="default", ticker=ticker)   # SQLite — UNIQUE(user_id, ticker)
    await source.add_ticker(ticker)                          # starts producing prices
    return {"ticker": ticker}
```

Order matters: **persist to SQLite before calling `add_ticker`**, so a crash between the two leaves a recoverable DB-only state (the ticker reappears in the simulator on next startup) rather than a cache entry with no watchlist row behind it.

### Flow: Removing a ticker

```python
@router.delete("/api/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = ticker.upper().strip()
    delete_watchlist_row(user_id="default", ticker=ticker)
    await source.remove_ticker(ticker)     # also clears the PriceCache entry (see §6.2, §7)
    return {"ticker": ticker}
```

### Edge case: ticker with an open position, removed from the watchlist

`PLAN.md` doesn't require the watchlist and positions tables to be in sync — a user can hold shares in a ticker no longer on their watchlist. Because `MarketDataSource.remove_ticker()` also evicts the entry from `PriceCache` (§3, §6.2, §7), portfolio valuation for that position would then see `cache.get_price(ticker) is None`. **The watchlist route must not blindly remove a ticker that has an open position** — check `positions` first, and if a position exists, keep the ticker tracked (simulator/poller keeps running for it) even though it no longer shows in the watchlist panel. This wasn't needed at the market-data layer itself, but is a hard requirement on whoever builds the watchlist/portfolio routes on top of it.

### Startup ticker set

The set passed to `source.start(tickers)` is **the union of the watchlist and any open positions** for `user_id="default"`, not just the watchlist — for the same reason: a position needs a live price to value even if its ticker was since removed from the watchlist.

---

## 12. Testing Strategy

**73 tests across 6 modules, 84% overall coverage on `app/market/`.**

| Module | Tests | Coverage | What it proves |
|---|---|---|---|
| `test_models.py` | 11 | 100% | `PriceUpdate` derived properties correct at edges (zero previous price, equal prices → "flat") |
| `test_cache.py` | 13 | 100% | Thread safety, version bump semantics, `get_all()` returns an independent copy |
| `test_simulator.py` | 17 | 98% | GBM math, Cholesky correlation, add/remove ticker rebuilds correctly |
| `test_simulator_source.py` | 10 | — (integration) | `SimulatorDataSource` start/stop/add/remove writes to a real `PriceCache` |
| `test_factory.py` | 7 | 100% | Env var presence/absence/whitespace selects the right source |
| `test_massive.py` | 13 | 56% (expected) | Snapshot parsing, timestamp conversion, error swallowing — API calls mocked |

### 12.1 Unit tests for `GBMSimulator`

```python
# tests/market/test_simulator.py
def test_step_moves_all_tracked_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    before = {t: sim.get_price(t) for t in sim.get_tickers()}
    sim.step()
    after = {t: sim.get_price(t) for t in sim.get_tickers()}
    assert before != after   # near-certain given nonzero sigma

def test_correlated_tickers_move_same_direction_more_often():
    # Statistical test: seed many steps, compare co-movement frequency of
    # two "tech" tickers (rho=0.6) against two cross-group tickers (rho=0.3)
    ...

def test_add_ticker_rebuilds_cholesky_without_losing_existing_prices():
    sim = GBMSimulator(tickers=["AAPL"])
    price_before = sim.get_price("AAPL")
    sim.add_ticker("TSLA")
    assert sim.get_price("AAPL") == price_before
    assert sim.get_price("TSLA") is not None

def test_unknown_ticker_gets_default_params_and_random_seed_price():
    sim = GBMSimulator(tickers=["ZZZZ"])
    assert 50.0 <= sim.get_price("ZZZZ") < 300.0

def test_random_event_can_move_price_more_than_typical_tick(monkeypatch):
    # Force event_probability=1.0 and assert a >=2% single-tick move occurs
    sim = GBMSimulator(tickers=["AAPL"], event_probability=1.0)
    before = sim.get_price("AAPL")
    sim.step()
    after = sim.get_price("AAPL")
    assert abs(after - before) / before >= 0.02
```

### 12.2 Unit tests for `PriceCache`

```python
# tests/market/test_cache.py
def test_first_update_is_flat():
    cache = PriceCache()
    update = cache.update("AAPL", 190.0)
    assert update.direction == "flat"
    assert update.previous_price == update.price

def test_version_increments_on_every_update():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.0)
    assert cache.version == v0 + 1

def test_get_all_returns_independent_snapshot():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    snap = cache.get_all()
    cache.update("AAPL", 200.0)
    assert snap["AAPL"].price == 190.0   # snapshot unaffected by later writes

def test_remove_clears_ticker():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    cache.remove("AAPL")
    assert cache.get("AAPL") is None
    assert "AAPL" not in cache
```

### 12.3 Integration test: `SimulatorDataSource`

```python
# tests/market/test_simulator_source.py
async def test_start_seeds_cache_before_first_tick():
    cache = PriceCache()
    source = SimulatorDataSource(cache, update_interval=100.0)  # long interval — no ticks expected
    await source.start(["AAPL"])
    assert cache.get_price("AAPL") is not None   # seeded synchronously in start(), not by the loop
    await source.stop()

async def test_stop_cancels_background_task_cleanly():
    cache = PriceCache()
    source = SimulatorDataSource(cache, update_interval=0.01)
    await source.start(["AAPL"])
    await asyncio.sleep(0.05)   # let a few ticks happen
    await source.stop()
    v = cache.version
    await asyncio.sleep(0.05)
    assert cache.version == v   # no further writes after stop()
```

### 12.4 Unit test: `MassiveDataSource` (mocked)

```python
# tests/market/test_massive.py
class FakeSnapshot:
    def __init__(self, ticker, price, ts_ms):
        self.ticker = ticker
        self.last_trade = SimpleNamespace(price=price, timestamp=ts_ms)

async def test_poll_once_updates_cache_and_converts_ms_to_seconds():
    cache = PriceCache()
    source = MassiveDataSource(api_key="fake", price_cache=cache)
    source._tickers = ["AAPL"]
    source._client = Mock()
    source._client.get_snapshot_all.return_value = [FakeSnapshot("AAPL", 190.5, 1_737_654_321_123)]

    await source._poll_once()

    update = cache.get("AAPL")
    assert update.price == 190.5
    assert update.timestamp == pytest.approx(1_737_654_321.123)

async def test_poll_failure_is_swallowed_not_raised():
    cache = PriceCache()
    source = MassiveDataSource(api_key="fake", price_cache=cache)
    source._tickers = ["AAPL"]
    source._client = Mock()
    source._client.get_snapshot_all.side_effect = Exception("401 unauthorized")

    await source._poll_once()   # must not raise
    assert cache.get("AAPL") is None
```

---

## 13. Error Handling & Edge Cases

### 13.1 Startup: empty watchlist

`GBMSimulator.step()` and `MassiveDataSource._poll_once()` both guard `n == 0` / `not self._tickers` and return immediately — a user who somehow starts with zero watched tickers gets a source that idles harmlessly rather than erroring.

### 13.2 Price cache miss during trade execution

`cache.get_price(ticker)` returns `None` for an untracked ticker. Trade execution (to be built) **must** treat `None` as "reject the order — no live price available" rather than defaulting to `0.0` or the last known DB price; a `None` price should never reach the buy/sell math.

### 13.3 Massive API key invalid or rate-limited

Handled uniformly by the broad `except Exception` in `_poll_once()` (§7) — logged as an error, loop continues, next poll retries. From the user's perspective a bad key just means a watchlist that never updates; there's no crash and no exception surfacing to the frontend. (A future improvement, out of scope for this component: surface a "market data degraded" flag alongside the SSE connection-status indicator described in `PLAN.md` §2 when polls have failed N times in a row.)

### 13.4 Thread safety under load

Every `PriceCache` mutation and multi-key read (`get_all`) happens under the single `Lock`; there is no scenario (many SSE clients reading concurrently with one source writing) where a reader can observe a torn/partial update, because `PriceUpdate` is frozen and swapped in atomically under the lock.

### 13.5 Simulator numerical precision

Prices are rounded to 2 decimal places both internally (`GBMSimulator.step()`) and again on cache write (`PriceCache.update()`); the double-round is intentional so that a `MassiveDataSource` value (already exchange-precision) and a simulator value are stored in exactly the same shape, and the frontend never needs source-specific display formatting.

---

## 14. Configuration Summary

| Env Var | Effect on market data |
|---|---|
| `MASSIVE_API_KEY` | Unset/empty (after `.strip()`) → `SimulatorDataSource`. Non-empty → `MassiveDataSource`. |
| — | No env var controls simulator tick rate or event probability; both are constructor defaults (`update_interval=0.5`, `event_probability=0.001`) since there's no product requirement to tune them at runtime. |

### Package `__init__.py` (recap of §1)

```python
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate", "PriceCache", "MarketDataSource",
    "create_market_data_source", "create_stream_router",
]
```

### Minimal end-to-end usage (recap of `MARKET_DATA_SUMMARY.md`)

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)          # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

update = cache.get("AAPL")           # PriceUpdate | None
price = cache.get_price("AAPL")      # float | None
snapshot = cache.get_all()           # dict[str, PriceUpdate]

await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

await source.stop()
```
