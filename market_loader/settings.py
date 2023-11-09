from pydantic.v1 import BaseSettings


class Settings(BaseSettings):
    token: str
    bot_token: str
    base_url: str = "https://invest-public-api.tinkoff.ru/rest/"
    share_by: str = "tinkoff.public.invest.api.contract.v1.InstrumentsService/ShareBy"
    get_candles: str = "tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
    find_instrument: str = "tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"

    bot_token: str
    debug_chat_id: int

    pg_name: str
    pg_user: str
    pg_password: str
    pg_host: str
    pg_port: str

    trade_start_hour = 7
    trade_end_hour = 20
    ema_cross_window = 4
    attempts_to_send_tg_msg = 10
    tg_send_timeout = 10
    attempts_to_tcs_request = 10
    tcs_request_timeout = 10
    deep_for_hour_candles = 60
    atr_period = 14
    mine_circle_sleep_time = 300

    @property
    def storage_url(self):
        return (
            f"postgresql+asyncpg://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_name}"
        )

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
