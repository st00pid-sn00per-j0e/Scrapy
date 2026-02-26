import csv
import os

from itemadapter import ItemAdapter


class QualifiedSitesCsvPipeline:
    fields = [
        "Website URL",
        "Company Name",
        "Phone Number",
        "Keyword Matches",
        "Emails",
    ]

    def __init__(self, settings):
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def open_spider(self, spider=None):
        output_path = self.settings.get("QUALIFIED_SITES_OUTPUT", "qualified_sites.csv")
        self.output_path = os.path.abspath(output_path)
        self.file = open(self.output_path, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.file,
            fieldnames=self.fields,
            extrasaction="ignore",
        )
        self.writer.writeheader()
        self.file.flush()

    def close_spider(self, spider=None):
        if hasattr(self, "file") and self.file and not self.file.closed:
            self.file.flush()
            self.file.close()

    def process_item(self, item, spider=None):
        adapter = ItemAdapter(item)
        row = {field: adapter.get(field, "") for field in self.fields}
        self.writer.writerow(row)
        self.file.flush()
        return item
