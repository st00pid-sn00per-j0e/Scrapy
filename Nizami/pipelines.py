import csv
import os

from itemadapter import ItemAdapter


class QualifiedSitesCsvPipeline:
    """Single CSV output — the only place output.csv is written."""

    fields = [
        "Website URL",
        "Company Name",
        "Phone Number",
        "Keywords - Team",
        "Specification",
        "Emails",
        "Person Name",
        "Job Title",
        "Service",
        "Relevancy",
        "Sales Hook",
        "Phone Numbers",
        "Twitter",
        "LinkedIn",
        "Facebook",
        "Instagram",
        "YouTube",
        "Other Social",
    ]

    def __init__(self, settings):
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def open_spider(self, spider=None):
        output_path = self.settings.get("QUALIFIED_SITES_OUTPUT", "output.csv")
        self.output_path = os.path.abspath(output_path)

        out_dir = os.path.dirname(self.output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        self.file = open(self.output_path, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.file,
            fieldnames=self.fields,
            extrasaction="ignore",
        )
        self.writer.writeheader()
        self.file.flush()
        if spider is not None:
            spider.logger.info("QualifiedSitesCsvPipeline writing to: %s", self.output_path)

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


class IncrementalCsvPipeline:

    def __init__(self, filename):
        self.filename = filename
        self.file = None
        self.writer = None
        self.headers_written = False

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            filename=crawler.settings.get('QUALIFIED_SITES_OUTPUT', 'output.csv')
        )

    def open_spider(self, spider=None):
        self.file = open(self.filename, 'a', newline='', encoding='utf-8')
        self.writer = None
        self.headers_written = (os.path.getsize(self.filename) > 0)

    def process_item(self, item, spider=None):
        if self.writer is None:
            fieldnames = list(item.keys())
            self.writer = csv.DictWriter(self.file, fieldnames=fieldnames, extrasaction='ignore')
        if not self.headers_written:
            self.writer.writeheader()
            self.headers_written = True
            self.file.flush()
        self.writer.writerow(dict(item))
        self.file.flush()
        return item

    def close_spider(self, spider=None):
        if self.file:
            self.file.close()