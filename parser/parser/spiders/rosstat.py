import os

import scrapy
from loguru import logger


class RosstatSpider(scrapy.Spider):
    name = "rosstat"
    allowed_domains = ["rosstat.gov.ru"]

    def create_folder_if_not_exist(self, folder_name):
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)
            logger.debug(f"Creating folder {folder_name}")
    def __init__(self, *args, **kwargs):
        self.url = "https://rosstat.gov.ru/statistic"
        self.domain = "https://rosstat.gov.ru"
        self.root_folder = "reports"

        self.create_folder_if_not_exist(self.root_folder)

        super().__init__(*args, **kwargs)

    def start_requests(self):
        yield scrapy.Request(self.url, self.get_folders)

    def get_folders(self, response):
        logger.info("Start parsing rosstat")
        items = response.css('.sidebar__item')
        logger.debug(f"the {len(items)} folders")
        for item in items:
            link = item.css('a::attr(href)').extract_first()
            name = item.css("*::text").extract()[1]
            yield scrapy.Request(link, self.get_field, cb_kwargs={"folder_name": name})

    def get_field(self, response, *args, **kwargs):
        folder_name = kwargs.get("folder_name")
        logger.debug(f"getting the fields of the folders {folder_name}")
        items = response.css('.sidebar__item')
        logger.debug(f"the {len(items)} folders")
        for item in items:
            link = item.css('a::attr(href)').extract_first()
            yield scrapy.Request(link, self.get_xlsx_links, cb_kwargs={"folder_name": folder_name})

    def get_xlsx_links(self, response, *args, **kwargs):
        folder_name = kwargs.get("folder_name")
        logger.debug(f"getting the links of the xlsx files from {response.url} of the folder {folder_name}")
        xlsx_links = response.css('a[href$=".xlsx"]::attr(href)').getall()

        for link in xlsx_links:
            yield scrapy.Request(self.domain + link, self.save_file, cb_kwargs={"folder_name": folder_name})

    def save_file(self, response, *args, **kwargs):
        folder_name = kwargs.get("folder_name")
        logger.debug(f"Saving the file from url {response.url} of the folder {folder_name}")

        folder = folder_name or "error"

        self.create_folder_if_not_exist(f"{self.root_folder}/{folder}")

        file_name = response.url.split("/")[-1]

        try:
            with open(f"{self.root_folder}/{folder}/{file_name}", "wb") as f:
                f.write(response.body)
        except Exception as e:
            logger.error(f"Error saving the file from url {response.url}, err ={e}")
