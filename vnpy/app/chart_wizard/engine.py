""""""
from typing import List
from datetime import datetime
from threading import Thread

from vnpy.event import Event, EventEngine
from vnpy.trader.event import EVENT_ORDER, EVENT_TRADE
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.constant import Interval
from vnpy.trader.object import HistoryRequest, ContractData, OrderData, TradeData
from vnpy.trader.rqdata import rqdata_client
from vnpy.trader.utility import extract_vt_symbol
from vnpy.trader.database import database_manager

APP_NAME = "ChartWizard"

EVENT_CHART_ORDER = "eChartOrder"
EVENT_CHART_TRADE = "eChartTrade"
EVENT_CHART_HISTORY = "eChartHistory"


class ChartWizardEngine(BaseEngine):
    """"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.register_event()

        rqdata_client.init()

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)

    def process_order_event(self, event: Event) -> None:
        """"""
        order = event.data
        database_manager.save_order([order])

    def process_trade_event(self, event: Event) -> None:
        """"""
        trade = event.data
        database_manager.save_trade([trade])

    def query_history(
        self,
        vt_symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        thread = Thread(
            target=self._query_history,
            args=[vt_symbol, interval, start, end]
        )
        thread.start()

    def query_order(
        self,
        vt_symbol: str,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        thread = Thread(
            target=self._query_order,
            args=[vt_symbol, start, end]
        )
        thread.start()

    def query_trade(
        self,
        vt_symbol: str,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        thread = Thread(
            target=self._query_trade,
            args=[vt_symbol, start, end]
        )
        thread.start()

    def _query_history(
        self,
        vt_symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        symbol, exchange = extract_vt_symbol(vt_symbol)

        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end
        )

        contract: ContractData = self.main_engine.get_contract(vt_symbol)
        if contract:
            if contract.history_data:
                data = self.main_engine.query_history(req, contract.gateway_name)
            else:
                data = rqdata_client.query_history(req)
        else:
            data = database_manager.load_bar_data(
                symbol,
                exchange,
                interval,
                start,
                end
            )

        event = Event(EVENT_CHART_HISTORY, data)
        self.event_engine.put(event)

    def _query_order(
        self,
        vt_symbol: str,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        symbol, exchange = extract_vt_symbol(vt_symbol)

        data = database_manager.load_order(
            symbol,
            exchange,
            start,
            end
        )

        event = Event(EVENT_CHART_ORDER, data)
        self.event_engine.put(event)

    def _query_trade(
        self,
        vt_symbol: str,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        symbol, exchange = extract_vt_symbol(vt_symbol)

        data = database_manager.load_trade(
            symbol,
            exchange,
            start,
            end
        )

        event = Event(EVENT_CHART_TRADE, data)
        self.event_engine.put(event)
