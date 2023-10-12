from bot.database import Database


class MarketDataLoader:

    def __init__(self, db: Database):
        self.db = db

    def load_data(self):
        pass