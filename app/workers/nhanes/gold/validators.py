import pandas as pd
from app.workers.nhanes.gold.base import GoldTask
from app.utils.logger import get_logger


class GoldValidators(GoldTask):

    def __init__(self):
        super().__init__()
        self._logger = get_logger(self.__class__.__name__)

    def insert_category_tracking(self, table, component, categories):
        for cat in categories:
            self.cursor.execute("""
                INSERT INTO public.data_check_gold
                (table_name, source_component, category)
                VALUES (%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (table, component, cat))
            self.conn.commit()

    def print_category_summary(self):
        df = pd.read_sql("""
            SELECT table_name, source_component,
                   COUNT(DISTINCT category) AS count
            FROM public.data_check_gold
            GROUP BY table_name, source_component
        """, self.conn)
        # Avoid printing raw dataframes to stdout; log a structured summary instead.
        rows = df.to_dict(orient="records")
        self._logger.info("Gold category summary", rows=rows)
