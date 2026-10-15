import os
import re
from app.config.category_mapping import DOMAIN_CATEGORIES
from app.workers.nhanes.silver.base import SilverTask


class SilverCategorizer(SilverTask):

    def categorize_file(self, filename):
        for domain, categories in DOMAIN_CATEGORIES.items():
            for category, pattern in categories.items():
                if re.search(pattern, filename, re.IGNORECASE):
                    return domain, category
        return "UNCATEGORIZED", "Unknown"

    def categorize_and_group_files(self, csv_files):
        categorized = {}
        for path in csv_files:
            filename = os.path.basename(path)
            domain, category = self.categorize_file(filename)
            categorized.setdefault(domain, {}).setdefault(category, []).append(path)
        return categorized
