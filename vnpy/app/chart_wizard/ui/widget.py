from copy import copy
from typing import Dict, List
from datetime import datetime, timedelta
from tzlocal import get_localzone

from vnpy.event import EventEngine, Event
from vnpy.chart import ChartWidget, CompositeChartWidget, CandleItem, VolumeItem
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore
from vnpy.trader.event import EVENT_ORDER, EVENT_TRADE, EVENT_TICK, EVENT_CONTRACT
from vnpy.trader.object import TickData, BarData, OrderData, TradeData, SubscribeRequest
from vnpy.trader.utility import BarGenerator
from vnpy.trader.constant import Interval

from vnpy.app.spread_trading.base import SpreadData, EVENT_SPREAD_DATA

from ..engine import APP_NAME, EVENT_CHART_HISTORY, ChartWizardEngine


class ChartWizardWidget(QtWidgets.QWidget):
    """"""
    signal_order = QtCore.pyqtSignal(Event)
    signal_trade = QtCore.pyqtSignal(Event)
    signal_tick = QtCore.pyqtSignal(Event)
    signal_spread = QtCore.pyqtSignal(Event)
    signal_history = QtCore.pyqtSignal(Event)
    signal_contract = QtCore.pyqtSignal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.chart_engine: ChartWizardEngine = main_engine.get_engine(APP_NAME)

        self.bgs: Dict[str, BarGenerator] = {}
        self.charts: Dict[str, ChartWidget] = {}

        self.init_ui()
        self.register_event()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("K线图表")

        self.tab: QtWidgets.QTabWidget = QtWidgets.QTabWidget()
        self.symbol_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

        self.vt_symbols = [c.vt_symbol.split('.')[0] for c in self.main_engine.get_all_contracts()]
        self.symbol_completer = QtWidgets.QCompleter(self.vt_symbols)
        self.symbol_completer.setFilterMode(QtCore.Qt.MatchContains)
        self.symbol_completer.setCompletionMode(self.symbol_completer.PopupCompletion)
        self.symbol_line.setCompleter(self.symbol_completer)

        self.type_combo = QtWidgets.QComboBox()
        self.type_combo.addItems(['简单图表', '复杂图表'])
        self.button = QtWidgets.QPushButton("新建图表")
        self.button.clicked.connect(self.new_chart)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(QtWidgets.QLabel("本地代码"))
        hbox.addWidget(self.symbol_line)
        hbox.addWidget(self.type_combo)
        hbox.addWidget(self.button)
        hbox.addStretch()

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox)
        vbox.addWidget(self.tab)

        self.setLayout(vbox)

    def create_chart(self) -> ChartWidget:
        """"""
        is_simple = (self.type_combo.currentIndex() == 0)
        if is_simple:
            chart = ChartWidget()
            chart.add_plot("candle", hide_x_axis=True)
            chart.add_plot("volume", maximum_height=200)
            chart.add_item(CandleItem, "candle", "candle")
            chart.add_item(VolumeItem, "volume", "volume")
            chart.add_cursor()
        else:
            chart = CompositeChartWidget()
        return chart

    def new_chart(self) -> None:
        """"""
        # Filter invalid vt_symbol
        vt_symbol = self.symbol_line.text()
        if not vt_symbol:
            return

        if vt_symbol in self.charts:
            return

        if "LOCAL" not in vt_symbol:
            contract = self.main_engine.get_contract(vt_symbol)
            if not contract:
                return

        # Create new chart
        self.bgs[vt_symbol] = BarGenerator(self.on_bar)

        chart = self.create_chart()
        self.charts[vt_symbol] = chart

        self.tab.addTab(chart, vt_symbol)

        # Query history data
        end = datetime.now(get_localzone())
        start = end - timedelta(days=5)

        self.chart_engine.query_history(
            vt_symbol,
            Interval.MINUTE,
            start,
            end
        )

        self.showMaximized()

    def register_event(self) -> None:
        """"""
        self.signal_order.connect(self.process_order_event)
        self.signal_trade.connect(self.process_trade_event)
        self.signal_tick.connect(self.process_tick_event)
        self.signal_spread.connect(self.process_spread_event)
        self.signal_history.connect(self.process_history_event)
        self.signal_contract.connect(self.process_contract_event)

        self.event_engine.register(EVENT_ORDER, self.signal_order.emit)
        self.event_engine.register(EVENT_TRADE, self.signal_trade.emit)
        self.event_engine.register(EVENT_TICK, self.signal_tick.emit)
        self.event_engine.register(EVENT_SPREAD_DATA, self.signal_spread.emit)
        self.event_engine.register(EVENT_CHART_HISTORY, self.signal_history.emit)
        self.event_engine.register(EVENT_CONTRACT, self.signal_contract.emit)

    def process_order_event(self, event: Event) -> None:
        """"""
        order: OrderData = event.data
        chart = self.charts[order.vt_symbol]
        if hasattr(chart, 'add_order'):
            chart.add_order(order)

    def process_trade_event(self, event: Event) -> None:
        """"""
        trade: TradeData = event.data
        chart = self.charts[trade.vt_symbol]
        if hasattr(chart, 'add_trade'):
            chart.add_trade(trade)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick: TickData = event.data
        bg = self.bgs.get(tick.vt_symbol, None)

        if bg:
            bg.update_tick(tick)

            chart = self.charts[tick.vt_symbol]
            bar = copy(bg.bar)
            bar.datetime = bar.datetime.replace(second=0, microsecond=0)
            chart.update_bar(bar)

    def process_history_event(self, event: Event) -> None:
        """"""
        history: List[BarData] = event.data
        if not history:
            return

        bar = history[0]
        chart = self.charts[bar.vt_symbol]
        chart.update_history(history)

        # Subscribe following data update
        contract = self.main_engine.get_contract(bar.vt_symbol)
        if contract:
            req = SubscribeRequest(
                contract.symbol,
                contract.exchange
            )
            self.main_engine.subscribe(req, contract.gateway_name)

    def process_spread_event(self, event: Event):
        """"""
        spread: SpreadData = event.data
        tick = spread.to_tick()

        bg = self.bgs.get(tick.vt_symbol, None)
        if bg:
            bg.update_tick(tick)

            chart = self.charts[tick.vt_symbol]
            bar = copy(bg.bar)
            bar.datetime = bar.datetime.replace(second=0, microsecond=0)
            chart.update_bar(bar)

    def process_contract_event(self, event: Event):
        """"""
        contract = event.data
        self.vt_symbols.append(contract.vt_symbol.split('.')[0])

        model = self.symbol_completer.model()
        model.setStringList(self.vt_symbols)

    def on_bar(self, bar: BarData):
        """"""
        chart = self.charts[bar.vt_symbol]
        chart.update_bar(bar)
