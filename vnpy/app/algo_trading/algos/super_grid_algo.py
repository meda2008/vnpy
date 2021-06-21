from vnpy.trader.utility import BarGenerator, ArrayManager, ceil_to, floor_to
from vnpy.trader.constant import Direction, Offset, OrderType, Interval
from vnpy.trader.object import TradeData, OrderData, TickData, BarData, PositionData
from vnpy.trader.engine import BaseEngine
from vnpy.app.algo_trading import AlgoTemplate


class SuperGridAlgo(AlgoTemplate):
    """"""

    display_name = "超级网格"

    default_setting = {
        "vt_symbol": "BTCUSDT.BINANCE",
        "lower_price": 40000.0,
        "upper_price": 60000.0,
        "trigger_price": 47000.0,
        "rise_percent": 1.0,
        "fall_down": 0.0,
        "fall_percent": 1.0,
        "rise_up": 0.0,
        "order_type": ["限价", "市价"],
        "order_volume": 0.1,
        "order_amount": 0.0,
        "max_position": 0.0,
        "min_position": 0.0,
        "multiple_order": True,
        "deadline": ["5日", "20日", "60日", "长期有效"],
        "give_up_bias": 0.0,
        "buy_offset": 0.0,
        "sell_offset": 0.0,
    }

    variables = [
        "pos",
        "vt_orderid",
        "touch_up",
        "touch_dn",
        "lowest_price",
        "highest_price",
        "trigger_price",
        "grid_sleep",
    ]

    def __init__(
            self,
            algo_engine: BaseEngine,
            algo_name: str,
            setting: dict
    ):
        """"""
        super().__init__(algo_engine, algo_name, setting)

        # Parameters
        self.vt_symbol: str = setting.get("vt_symbol")
        self.lower_price: float = setting.get("lower_price")
        self.upper_price: float = setting.get("upper_price")
        self.trigger_price: float = setting.get("trigger_price")
        self.rise_percent: float = setting.get("rise_percent")
        self.fall_down: float = setting.get("fall_down")
        self.fall_percent: float = setting.get("fall_percent")
        self.rise_up: float = setting.get("rise_up")
        self.order_type: OrderType = OrderType(setting.get("order_type", "限价"))
        self.order_volume: float = setting.get("order_volume")
        self.order_amount: float = setting.get("order_amount")
        self.max_position: float = setting.get("max_position")
        self.min_position: float = setting.get("min_position")
        self.multiple_order: bool = setting.get("multiple_order")
        self.deadline: str = setting.get("deadline")
        self.give_up_bias: float = setting.get("give_up_bias")
        self.buy_offset: float = setting.get("buy_offset")
        self.sell_offset: float = setting.get("sell_offset")

        # Variables
        self.pos = 0
        self.vt_orderid = ""
        self.last_tick = None
        self.grid_sleep = False
        self.touch_up = False
        self.touch_dn = False
        self.lowest_price = None
        self.highest_price = None

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        self.subscribe(self.vt_symbol)
        self.put_parameters_event()
        self.put_variables_event()

    def on_init(self):
        """"""
        # 回测时使用
        if hasattr(self.algo_engine, "load_bar"):
            self.buy = self._buy
            self.sell = self._sell
            self.algo_engine.load_bar(self.vt_symbol, 10, Interval.MINUTE, self.on_bar, True)

    def on_start(self):
        """"""
        # 回测时使用
        self.active = True

    def on_bar(self, bar: BarData):
        """"""
        # 回测时使用
        if hasattr(self.algo_engine, "load_bar"):
            # 获得最新价,买一价,卖一价
            last_price = bar.close_price
            ask_price = bar.close_price
            bid_price = bar.close_price

            # 执行网格逻辑
            self.on_logic(last_price, ask_price, bid_price)

    def on_tick(self, tick: TickData):
        """"""
        self.last_tick = tick
        self.bg.update_tick(tick)

        # 获得最新价,买一价,卖一价
        last_price = self.last_tick.last_price
        ask_price = self.last_tick.ask_price_1
        bid_price = self.last_tick.bid_price_1

        # 执行网格逻辑
        self.on_logic(last_price, ask_price, bid_price)

    def on_logic(self, last_price: float, ask_price: float, bid_price: float):
        """"""
        # 如果超出网格设置的上下沿则停止网格
        if (last_price > self.upper_price) or (last_price < self.lower_price):
            self.cancel_all()
            if not self.grid_sleep:
                self.grid_sleep = True
                self.put_variables_event()
                self.write_log(f"休眠: {last_price}, {self.upper_price}, {self.lower_price}")
        # 落入网格区间重新打开网格
        else:
            if self.grid_sleep:
                self.grid_sleep = False
                self.put_variables_event()
                self.write_log(f"运行: {last_price}, {self.upper_price}, {self.lower_price}")

        # 如果是休眠状态则不执行网格逻辑
        if self.grid_sleep:
            return

        # 最新价高于触发价则执行上涨卖出逻辑判断
        if last_price > self.trigger_price:
            rise_pct = (last_price - self.trigger_price) / self.trigger_price * 100
            # 上涨百分比达到设定值则打开回落判断开关,并设置最高价
            if rise_pct >= self.rise_percent:
                self.touch_up = True
                if self.highest_price is None:
                    self.highest_price = last_price
                else:
                    self.highest_price = max(last_price, self.highest_price)

        # 最新价低于触发价则执行下跌买入逻辑判断
        elif last_price < self.trigger_price:
            fall_pct = (self.trigger_price - last_price) / self.trigger_price * 100
            # 下跌百分比达到设定值则打开反弹判断开关,并设置最低价
            if fall_pct >= self.fall_percent:
                self.touch_dn = True
                if self.lowest_price is None:
                    self.lowest_price = last_price
                else:
                    self.lowest_price = min(last_price, self.lowest_price)

        # 上涨回落卖出逻辑
        if self.touch_up:
            fall_dn_pct = (self.highest_price - last_price) / self.highest_price * 100
            # 回落百分比达到设定值则执行回落卖出逻辑
            if fall_dn_pct >= self.fall_down:
                # 计算卖出价格,默认使用市价单,买一价
                sell_price = bid_price
                # 如果委托类型设定是限价单,则用最新成交价减去卖出价格偏移提高成交概率
                if self.order_type == OrderType.LIMIT:
                    sell_price = last_price - self.sell_offset
                # 计算卖出数量,默认使用设定的每笔委托数量
                sell_volume = self.order_volume
                # 如果同时设定了每笔委托金额,则优先使用每笔委托金额来计算委托数量
                if self.order_amount > 0:
                    sell_volume = sell_price / self.order_amount
                # 如果设定了倍数委托则用涨幅倍数重新计算委托数量
                if self.multiple_order:
                    rise_pct = (last_price - self.trigger_price) / self.trigger_price * 100
                    multiple = rise_pct / self.rise_percent
                    multiple = 1 if multiple < 1 else floor_to(multiple, 1)
                    sell_volume = sell_volume * multiple
                # 如果设定了最小底仓则重新计算委托数量
                if self.min_position > 0:
                    if self.pos > self.min_position:
                        sell_volume = min(sell_volume, self.pos - self.min_position)
                    else:
                        sell_volume = 0
                # 如果设定了偏差控制则执行偏差判断逻辑
                if self.give_up_bias > 0:
                    bias = (last_price - self.trigger_price) / self.trigger_price * 100
                    if 0 < bias < self.give_up_bias:
                        self.sell(self.vt_symbol, sell_price, sell_volume, self.order_type, Offset.CLOSE)
                        self.touch_up = False
                        self.highest_price = None
                        self.trigger_price = sell_price
                else:
                    print('UP: ', last_price, self.trigger_price, self.highest_price, fall_dn_pct)  # TODO
                    self.sell(self.vt_symbol, sell_price, sell_volume, self.order_type, Offset.CLOSE)
                    self.touch_up = False
                    self.highest_price = None
                    self.trigger_price = sell_price

        # 下跌反弹买入逻辑
        if self.touch_dn:
            rise_up_pct = (last_price - self.lowest_price) / self.lowest_price * 100
            # 反弹百分比达到设定值则执行反弹买入逻辑
            if rise_up_pct >= self.rise_up:
                # 计算买入价格,默认使用市价单,卖一价
                buy_price = ask_price
                # 如果委托类型设定是限价单,则用最新成交价加上买入价格偏移提高成交概率
                if self.order_type == OrderType.LIMIT:
                    buy_price = last_price + self.buy_offset
                # 计算买入数量,默认使用设定的每笔委托数量
                buy_volume = self.order_volume
                # 如果同时设定了每笔委托金额,则优先使用每笔委托金额来计算委托数量
                if self.order_amount > 0:
                    buy_volume = buy_price / self.order_amount
                # 如果设定了倍数委托则用跌幅倍数重新计算委托数量
                if self.multiple_order:
                    fall_pct = (self.trigger_price - last_price) / self.trigger_price * 100
                    multiple = fall_pct / self.fall_percent
                    multiple = 1 if multiple < 1 else ceil_to(multiple, 1)
                    buy_volume = buy_volume * multiple
                # 如果设定了最大持仓则重新计算委托数量
                if self.max_position > 0:
                    buy_volume = min(buy_volume, self.max_position - self.pos)
                # 如果设定了偏差控制则执行偏差判断逻辑
                if self.give_up_bias > 0:
                    bias = (self.trigger_price - last_price) / self.trigger_price * 100
                    if 0 < bias < self.give_up_bias:
                        self.buy(self.vt_symbol, buy_price, buy_volume, self.order_type, Offset.OPEN)
                        self.touch_dn = False
                        self.lowest_price = None
                        self.trigger_price = buy_price
                else:
                    print('DN: ', last_price, self.trigger_price, self.lowest_price, rise_up_pct)  # TODO
                    self.buy(self.vt_symbol, buy_price, buy_volume, self.order_type, Offset.OPEN)
                    self.touch_dn = False
                    self.lowest_price = None
                    self.trigger_price = buy_price

        # Update UI
        self.put_variables_event()

    def on_order(self, order: OrderData):
        """"""
        if not order.is_active():
            self.vt_orderid = ""
            self.put_variables_event()

    def on_trade(self, trade: TradeData):
        """"""
        if trade.direction == Direction.LONG:
            self.pos += trade.volume
            self.touch_up = False
            self.highest_price = None
        else:
            self.pos -= trade.volume
            self.touch_dn = False
            self.lowest_price = None

        self.trigger_price = trade.price

        self.put_variables_event()

    def on_position(self, pos: PositionData):
        """"""
        self.pos = pos.volume
        if self.trigger_price <= 0:
            self.trigger_price = pos.price

    def _buy(
        self,
        vt_symbol,
        price,
        volume,
        order_type: OrderType = OrderType.LIMIT,
        offset: Offset = Offset.NONE
    ):
        """回测时使用"""
        if not self.active:
            return

        msg = f"委托买入{vt_symbol}：{volume}@{price}"
        self.write_log(msg)

        return self.algo_engine.send_order(
            self,
            Direction.LONG,
            offset,
            price,
            volume,
            False,
            False,
            False
        )

    def _sell(
        self,
        vt_symbol,
        price,
        volume,
        order_type: OrderType = OrderType.LIMIT,
        offset: Offset = Offset.NONE
    ):
        """回测时使用"""
        if not self.active:
            return

        msg = f"委托卖出{vt_symbol}：{volume}@{price}"
        self.write_log(msg)

        return self.algo_engine.send_order(
            self,
            Direction.SHORT,
            offset,
            price,
            volume,
            False,
            False,
            False
        )
