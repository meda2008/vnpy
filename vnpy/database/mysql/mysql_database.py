""""""
from datetime import datetime
from typing import List

from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    FloatField, IntegerField,
    Model,
    MySQLDatabase as PeeweeMySQLDatabase,
    ModelSelect,
    ModelDelete,
    chunked,
    fn
)

from vnpy.trader.constant import (
    Exchange,
    Interval,
    OrderType,
    Direction,
    Offset,
    Status
)
from vnpy.trader.object import (
    BarData,
    TickData,
    OrderData,
    TradeData
)
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


db = PeeweeMySQLDatabase(
    database=SETTINGS["database.database"],
    user=SETTINGS["database.user"],
    password=SETTINGS["database.password"],
    host=SETTINGS["database.host"],
    port=SETTINGS["database.port"]
)


class DbBarData(Model):
    """"""

    id = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    datetime: datetime = DateTimeField()
    interval: str = CharField()

    volume: float = FloatField()
    open_interest: float = FloatField()
    open_price: float = FloatField()
    high_price: float = FloatField()
    low_price: float = FloatField()
    close_price: float = FloatField()

    class Meta:
        database = db
        indexes = ((("symbol", "exchange", "interval", "datetime"), True),)


class DbTickData(Model):
    """"""

    id = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    datetime: datetime = DateTimeField()

    name: str = CharField()
    volume: float = FloatField()
    open_interest: float = FloatField()
    last_price: float = FloatField()
    last_volume: float = FloatField()
    limit_up: float = FloatField()
    limit_down: float = FloatField()

    open_price: float = FloatField()
    high_price: float = FloatField()
    low_price: float = FloatField()
    pre_close: float = FloatField()

    bid_price_1: float = FloatField()
    bid_price_2: float = FloatField(null=True)
    bid_price_3: float = FloatField(null=True)
    bid_price_4: float = FloatField(null=True)
    bid_price_5: float = FloatField(null=True)

    ask_price_1: float = FloatField()
    ask_price_2: float = FloatField(null=True)
    ask_price_3: float = FloatField(null=True)
    ask_price_4: float = FloatField(null=True)
    ask_price_5: float = FloatField(null=True)

    bid_volume_1: float = FloatField()
    bid_volume_2: float = FloatField(null=True)
    bid_volume_3: float = FloatField(null=True)
    bid_volume_4: float = FloatField(null=True)
    bid_volume_5: float = FloatField(null=True)

    ask_volume_1: float = FloatField()
    ask_volume_2: float = FloatField(null=True)
    ask_volume_3: float = FloatField(null=True)
    ask_volume_4: float = FloatField(null=True)
    ask_volume_5: float = FloatField(null=True)

    class Meta:
        database = db
        indexes = ((("symbol", "exchange", "datetime"), True),)


class DbBarOverview(Model):
    """"""

    id = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    interval: str = CharField()
    count: int = IntegerField()
    start: datetime = DateTimeField()
    end: datetime = DateTimeField()

    class Meta:
        database = db
        indexes = ((("symbol", "exchange", "interval"), True),)


class DbOrderData(Model):
    """"""

    id = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    datetime: datetime = DateTimeField()

    orderid: str = CharField()
    type: str = CharField()
    direction: str = CharField()
    offset: str = CharField()
    price: float = FloatField()
    volume: float = FloatField()
    traded: float = FloatField()
    status: str = CharField()
    reference: str = CharField()

    class Meta:
        database = db
        indexes = ((("symbol", "exchange", "datetime", "orderid"), True),)


class DbTradeData(Model):
    """"""

    id = AutoField()

    symbol: str = CharField()
    exchange: str = CharField()
    datetime: datetime = DateTimeField()

    orderid: str = CharField()
    tradeid: str = CharField()
    direction: str = CharField()
    offset: str = CharField()
    price: float = FloatField()
    volume: float = FloatField()

    class Meta:
        database = db
        indexes = ((("symbol", "exchange", "datetime", "tradeid"), True),)


class MysqlDatabase(BaseDatabase):
    """"""

    def __init__(self) -> None:
        """"""
        self.db = db
        self.db.connect()
        self.db.create_tables([DbBarData, DbTickData, DbBarOverview, DbOrderData, DbTradeData])

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """"""
        # Store key parameters
        bar = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange
        interval = bar.interval

        # Convert bar object to dict and adjust timezone
        data = []

        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d = bar.__dict__
            d["exchange"] = d["exchange"].value
            d["interval"] = d["interval"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        # Upsert data into database
        with self.db.atomic():
            for c in chunked(data, 50):
                DbBarData.insert_many(c).on_conflict_replace().execute()

        # Update bar overview
        overview: DbBarOverview = DbBarOverview.get_or_none(
            DbBarOverview.symbol == symbol,
            DbBarOverview.exchange == exchange.value,
            DbBarOverview.interval == interval.value,
        )

        if not overview:
            overview = DbBarOverview()
            overview.symbol = symbol
            overview.exchange = exchange.value
            overview.interval = interval.value
            overview.start = bars[0].datetime
            overview.end = bars[-1].datetime
            overview.count = len(bars)
        else:
            overview.start = min(bars[0].datetime, overview.start)
            overview.end = max(bars[-1].datetime, overview.end)

            s: ModelSelect = DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
            )
            overview.count = s.count()

        overview.save()

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """"""
        # Convert bar object to dict and adjust timezone
        data = []

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

            d = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        # Upsert data into database
        with self.db.atomic():
            for c in chunked(data, 50):
                DbTickData.insert_many(c).on_conflict_replace().execute()

    def save_order(self, orders: List[OrderData]) -> bool:
        """"""
        data = []

        for order in orders:
            order.datetime = convert_tz(order.datetime)

            d = order.__dict__
            d["exchange"] = d["exchange"].value
            d["direction"] = d["direction"].value
            d["offset"] = d["offset"].value
            d["status"] = d["status"].value
            d["type"] = d["type"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        # Upsert data into database
        with self.db.atomic():
            for c in chunked(data, 50):
                DbOrderData.insert_many(c).on_conflict_replace().execute()

    def save_trade(self, trades: List[TradeData]) -> bool:
        """"""
        data = []

        for trade in trades:
            trade.datetime = convert_tz(trade.datetime)

            d = trade.__dict__
            d["exchange"] = d["exchange"].value
            d["direction"] = d["direction"].value
            d["offset"] = d["offset"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        # Upsert data into database
        with self.db.atomic():
            for c in chunked(data, 50):
                DbTradeData.insert_many(c).on_conflict_replace().execute()

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        s: ModelSelect = (
            DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
                & (DbBarData.datetime >= start)
                & (DbBarData.datetime <= end)
            ).order_by(DbBarData.datetime)
        )

        vt_symbol = f"{symbol}.{exchange.value}"
        bars: List[BarData] = []
        for db_bar in s:
            db_bar.datetime = DB_TZ.localize(db_bar.datetime)
            db_bar.exchange = Exchange(db_bar.exchange)
            db_bar.interval = Interval(db_bar.interval)
            db_bar.gateway_name = "DB"
            db_bar.vt_symbol = vt_symbol
            bars.append(db_bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """"""
        s: ModelSelect = (
            DbTickData.select().where(
                (DbTickData.symbol == symbol)
                & (DbTickData.exchange == exchange.value)
                & (DbTickData.datetime >= start)
                & (DbTickData.datetime <= end)
            ).order_by(DbTickData.datetime)
        )

        vt_symbol = f"{symbol}.{exchange.value}"
        ticks: List[TickData] = []
        for db_tick in s:
            db_tick.datetime = DB_TZ.localize(db_tick.datetime)
            db_tick.exchange = Exchange(db_tick.exchange)
            db_tick.gateway_name = "DB"
            db_tick.vt_symbol = vt_symbol
            ticks.append(db_tick)

        return ticks

    def load_order(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[OrderData]:
        """"""
        s: ModelSelect = (
            DbOrderData.select().where(
                (DbOrderData.symbol == symbol)
                & (DbOrderData.exchange == exchange.value)
                & (DbOrderData.datetime >= start)
                & (DbOrderData.datetime <= end)
            ).order_by(DbOrderData.datetime)
        )

        vt_symbol = f"{symbol}.{exchange.value}"
        orders: List[OrderData] = []
        for db_order in s:
            db_order.datetime = DB_TZ.localize(db_order.datetime)
            db_order.exchange = Exchange(db_order.exchange)
            db_order.direction = Direction(db_order.direction)
            db_order.offset = Offset(db_order.offset)
            db_order.type = OrderType(db_order.type)
            db_order.status = Status(db_order.status)
            db_order.gateway_name = "DB"
            db_order.vt_symbol = vt_symbol
            orders.append(db_order)

        return orders

    def load_trade(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TradeData]:
        """"""
        s: ModelSelect = (
            DbTradeData.select().where(
                (DbTradeData.symbol == symbol)
                & (DbTradeData.exchange == exchange.value)
                & (DbTradeData.datetime >= start)
                & (DbTradeData.datetime <= end)
            ).order_by(DbTradeData.datetime)
        )

        vt_symbol = f"{symbol}.{exchange.value}"
        trades: List[TradeData] = []
        for db_trade in s:
            db_trade.datetime = DB_TZ.localize(db_trade.datetime)
            db_trade.exchange = Exchange(db_trade.exchange)
            db_trade.direction = Direction(db_trade.direction)
            db_trade.offset = Offset(db_trade.offset)
            db_trade.gateway_name = "DB"
            db_trade.vt_symbol = vt_symbol
            trades.append(db_trade)

        return trades

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        d: ModelDelete = DbBarData.delete().where(
            (DbBarOverview.symbol == symbol)
            & (DbBarOverview.exchange == exchange.value)
            & (DbBarOverview.interval == interval.value)
        )
        count = d.execute()

        # Delete bar overview
        d2: ModelDelete = DbBarOverview.delete().where(
            (DbBarData.symbol == symbol)
            & (DbBarData.exchange == exchange.value)
            & (DbBarData.interval == interval.value)
        )
        d2.execute()
        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """"""
        d: ModelDelete = DbTickData.delete().where(
            (DbTickData.symbol == symbol)
            & (DbTickData.exchange == exchange.value)
        )
        count = d.execute()
        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """
        Return data avaible in database.
        """
        # Init bar overview for old version database
        data_count = DbBarData.select().count()
        overview_count = DbBarOverview.select().count()
        if data_count and not overview_count:
            self.init_bar_overview()

        s: ModelSelect = DbBarOverview.select()
        overviews = []
        for overview in s:
            overview.exchange = Exchange(overview.exchange)
            overview.interval = Interval(overview.interval)
            overview.start = DB_TZ.localize(overview.start)
            overview.end = DB_TZ.localize(overview.end)
            overviews.append(overview)
        return overviews

    def init_bar_overview(self) -> None:
        """
        Init overview table if not exists.
        """
        s: ModelSelect = (
            DbBarData.select(
                DbBarData.symbol,
                DbBarData.exchange,
                DbBarData.interval,
                fn.COUNT(DbBarData.id).alias("count")
            ).group_by(
                DbBarData.symbol,
                DbBarData.exchange,
                DbBarData.interval
            )
        )

        for data in s:
            overview = DbBarOverview()
            overview.symbol = data.symbol
            overview.exchange = data.exchange
            overview.interval = data.interval
            overview.count = data.count

            start_bar: DbBarData = (
                DbBarData.select()
                .where(
                    (DbBarData.symbol == data.symbol)
                    & (DbBarData.exchange == data.exchange)
                    & (DbBarData.interval == data.interval)
                )
                .order_by(DbBarData.datetime.asc())
                .first()
            )
            overview.start = start_bar.datetime

            end_bar: DbBarData = (
                DbBarData.select()
                .where(
                    (DbBarData.symbol == data.symbol)
                    & (DbBarData.exchange == data.exchange)
                    & (DbBarData.interval == data.interval)
                )
                .order_by(DbBarData.datetime.desc())
                .first()
            )
            overview.end = end_bar.datetime

            overview.save()


database_manager = MysqlDatabase()
